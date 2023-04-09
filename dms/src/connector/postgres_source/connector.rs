use crate::connector::connector::Connector;
use crate::connector::postgres_source::config::PostgresSourceConfig;
use crate::connector::postgres_source::event::{Action, Column, RawPostgresEvent};
use crate::connector::postgres_source::pgoutput::events::{PgOutputEvent, RelationEvent};
use crate::connector::postgres_source::pgoutput::utils::{keep_alive, parse_pgoutput_event};
use crate::error::generic::{DMSRError, DMSRResult};
use crate::error::missing_value::MissingValueError;
use crate::event::event::{DataType, Field, JSONChangeEvent, Operation, Schema};
use crate::kafka::kafka::Kafka;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use std::collections::HashMap;
use std::pin::Pin;
use tokio_postgres::{Client, CopyBothDuplex, NoTls, SimpleQueryMessage, SimpleQueryRow};

pub struct PostgresSourceConnector {
    config: PostgresSourceConfig,
    client: Option<Client>,
    connector_name: String,
    relation_map: HashMap<u32, RelationEvent>,
}

#[async_trait]
impl Connector for PostgresSourceConnector {
    type Config = PostgresSourceConfig;

    fn new(connector_name: String, config: &PostgresSourceConfig) -> DMSRResult<Box<Self>> {
        Ok(Box::new(PostgresSourceConnector {
            config: config.clone(),
            client: None,
            connector_name,
            relation_map: HashMap::new(),
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
        self.create_publication().await?;
        let (lsn, slot_name) = self.create_replication_slot().await?;
        let mut stream = self.start_replication_slot(lsn, slot_name).await?;

        while let Some(event) = stream.next().await {
            let event = event?;
            let event = event.as_ref();

            match &event[0] {
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
                            self.relation_map.insert(event.relation_id, event);
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
    async fn execute_query(&self, query: String) -> DMSRResult<Vec<SimpleQueryRow>> {
        let res = self
            .client
            .as_ref()
            .ok_or(MissingValueError::new("Client is not initialized"))?
            .simple_query(query.as_str())
            .await?;

        let res = res
            .into_iter()
            .filter_map(|m| match m {
                SimpleQueryMessage::Row(r) => Some(r),
                _ => None,
            })
            .collect::<Vec<SimpleQueryRow>>();

        Ok(res)
    }

    async fn create_publication(&self) -> DMSRResult<()> {
        let publication_name = format!("dmsr_pub_all_tables_{}", self.connector_name);
        let query = format!("CREATE PUBLICATION {} FOR ALL TABLES", publication_name);

        let res = self.execute_query(query).await;
        if res.is_err() {
            println!("Publication already exists");
        }

        Ok(())
    }

    async fn create_replication_slot(&self) -> DMSRResult<(String, String)> {
        let slot_name = format!("dmsr_slot_{}", self.connector_name);
        let slot_query = format!("CREATE_REPLICATION_SLOT {} LOGICAL \"pgoutput\"", slot_name);
        let slot_query_res = self.execute_query(slot_query).await;

        let lsn: String;
        if let Ok(res) = slot_query_res {
            lsn = res[0]
                .get("consistent_point")
                .ok_or(DMSRError::PostgresError("No LSN found".into()))?
                .to_string();
        } else {
            let find_slot_query = format!(
                "SELECT * FROM pg_replication_slots WHERE slot_name = '{}'",
                slot_name
            );

            let find_slot_res = self.execute_query(find_slot_query).await?;
            lsn = find_slot_res[0]
                .get("confirmed_flush_lsn")
                .ok_or(DMSRError::PostgresError("No LSN found".into()))?
                .to_string();
        }

        Ok((lsn, slot_name))
    }

    async fn start_replication_slot(
        &mut self,
        lsn: String,
        slot_name: String,
    ) -> DMSRResult<Pin<Box<CopyBothDuplex<Bytes>>>> {
        let client = self
            .client
            .as_mut()
            .ok_or(MissingValueError::new("Client is not initialized"))?;

        let query = format!(
            "START_REPLICATION SLOT {} LOGICAL {} (\"proto_version\" '1', \"publication_names\" 'all_tables')",
            slot_name,
            lsn
        );

        let stream = client.copy_both_simple::<Bytes>(&query).await?;
        let stream = Box::pin(stream);

        Ok(stream)
    }

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
