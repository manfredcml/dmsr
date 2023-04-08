use crate::connector::connector::Connector;
use crate::connector::postgres_source::config::PostgresSourceConfig;
use crate::connector::postgres_source::event::{Action, Column, RawPostgresEvent};
use crate::connector::postgres_source::pgoutput::events::PgOutputEvent;
use crate::connector::postgres_source::pgoutput::utils::{keep_alive, parse_pgoutput_event};
use crate::error::generic::{DMSRError, DMSRResult};
use crate::error::missing_value::MissingValueError;
use crate::event::event::{DataType, Field, JSONChangeEvent, Operation, Schema};
use crate::kafka::kafka::Kafka;
use async_trait::async_trait;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use bytes::Bytes;
use futures::{future, ready, Sink, StreamExt};
use std::collections::HashMap;
use std::io::Cursor;
use std::pin::Pin;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_postgres::{Client, CopyBothDuplex, NoTls, SimpleQueryMessage, SimpleQueryRow};

pub struct PostgresSourceConnector {
    config: PostgresSourceConfig,
    client: Option<Client>,
    connector_name: String,
    topic_prefix: String,
}

#[async_trait]
impl Connector for PostgresSourceConnector {
    type Config = PostgresSourceConfig;

    fn new(
        connector_name: String,
        topic_prefix: String,
        config: &PostgresSourceConfig,
    ) -> DMSRResult<Box<Self>> {
        Ok(Box::new(PostgresSourceConnector {
            config: config.clone(),
            client: None,
            connector_name,
            topic_prefix,
        }))
    }

    async fn connect(&mut self) -> DMSRResult<()> {
        let endpoint = format!(
            "host={} port={} user={} password={} replication=database",
            self.config.host, self.config.port, self.config.user, self.config.password
        );

        let (client, connection) = tokio_postgres::connect(endpoint.as_str(), NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        self.client = Some(client);
        Ok(())
    }

    async fn stream(&mut self, mut kafka: Kafka) -> DMSRResult<()> {
        let client = self
            .client
            .as_mut()
            .ok_or(MissingValueError::new("Client is not initialized"))?;

        let slot_name = format!("dmsr_slot_{}", self.connector_name);
        let slot_query = format!("CREATE_REPLICATION_SLOT {} LOGICAL \"pgoutput\"", slot_name);

        let slot_query_res = client.simple_query(slot_query.as_str()).await;

        let lsn: String;
        if let Ok(msg) = slot_query_res {
            let res = msg
                .into_iter()
                .filter_map(|m| match m {
                    SimpleQueryMessage::Row(r) => Some(r),
                    _ => None,
                })
                .collect::<Vec<SimpleQueryRow>>();

            lsn = res[0]
                .get("consistent_point")
                .ok_or(DMSRError::PostgresError("No LSN found".into()))?
                .to_string();
        } else {
            let slot_query = format!(
                "SELECT * FROM pg_replication_slots WHERE slot_name = '{}'",
                slot_name
            );

            let slot_query_res = client.simple_query(slot_query.as_str()).await?;

            let res = slot_query_res
                .into_iter()
                .filter_map(|m| match m {
                    SimpleQueryMessage::Row(r) => Some(r),
                    _ => None,
                })
                .collect::<Vec<SimpleQueryRow>>();

            lsn = res[0]
                .get("confirmed_flush_lsn")
                .ok_or(DMSRError::PostgresError("No LSN found".into()))?
                .to_string();
        }

        let query = format!(
            "START_REPLICATION SLOT {} LOGICAL {} (\"proto_version\" '1', \"publication_names\" 'all_tables')",
            slot_name,
            lsn
        );

        let stream = client.copy_both_simple::<Bytes>(&query).await?;
        let mut stream = Box::pin(stream);

        while let Some(event) = stream.next().await {
            let event = event?;
            let first_byte = &event[0];
            let event = event.as_ref();

            match first_byte {
                b'k' => {
                    keep_alive(event, &mut stream).await?;
                }
                b'w' => {
                    let pg_output_event = parse_pgoutput_event(event)?;

                    match pg_output_event {
                        PgOutputEvent::Begin(event) => {
                            println!("Begin: {:?}", event);
                        }
                        PgOutputEvent::Commit(event) => {
                            println!("Commit: {:?}", event);
                        }
                        PgOutputEvent::Relation(event) => {
                            println!("Relation: {:?}", event);
                        }
                        PgOutputEvent::Insert(event) => {
                            println!("Insert: {:?}", event);
                        }
                        PgOutputEvent::Update(event) => {
                            println!("Update: {:?}", event);
                        }
                        PgOutputEvent::Delete(event) => {
                            println!("Delete: {:?}", event);
                        }
                        PgOutputEvent::Truncate(event) => {
                            println!("Truncate: {:?}", event);
                        }
                        PgOutputEvent::Origin => {}
                    }

                    println!("====================================")
                }
                _ => {}
            }
        }

        Ok(())
    }
}

impl PostgresSourceConnector {
    fn process_event(&self, event: &Bytes) -> DMSRResult<Option<JSONChangeEvent>> {
        let b = &event[25..];
        let s = std::str::from_utf8(b)?;

        let raw_event: RawPostgresEvent = serde_json::from_str(s)?;
        if raw_event.action == Action::B || raw_event.action == Action::C {
            return Ok(None);
        }

        println!("Parsed raw event: {:?}", raw_event);

        // Get operation
        let operation = match raw_event.action {
            Action::I => Operation::Create,
            Action::U => Operation::Update,
            Action::D => Operation::Delete,
            _ => return Ok(None),
        };

        // Get payload
        let mut columns: Vec<Column> = vec![];
        if operation == Operation::Create {
            columns = raw_event.columns.unwrap_or_default();
        } else {
            columns = raw_event.identity.unwrap_or_default();
        }

        let mut payload: HashMap<String, serde_json::Value> = HashMap::new();
        let mut fields: Vec<Field> = vec![];
        for c in columns {
            let column_name = c.name.clone();
            payload.insert(c.name, c.value);

            let data_type: DataType = match c.r#type.as_str() {
                "integer" => DataType::Int32,
                s if s.starts_with("character varying") => DataType::String,
                _ => return Ok(None),
            };

            fields.push(Field {
                r#type: data_type,
                optional: false,
                field: column_name,
            });
        }

        let table = raw_event.table.unwrap_or_default();
        let schema = raw_event.schema.unwrap_or_default();
        if table.is_empty() || schema.is_empty() {
            return Ok(None);
        }

        let pk: Vec<String> = raw_event
            .pk
            .unwrap_or_default()
            .iter()
            .map(|s| s.name.clone())
            .collect();

        let json_event = JSONChangeEvent {
            source_connector: self.connector_name.clone(),
            schema: Schema {
                r#type: DataType::Struct,
                fields,
            },
            payload,
            table: format!("{}.{}", schema, table),
            op: operation,
            pk,
        };

        Ok(Some(json_event))
    }
}
