use crate::connector::connector::Connector;
use crate::connector::postgres_source::config::PostgresSourceConfig;
use crate::connector::postgres_source::event::{Action, RawPostgresEvent};
use crate::error::generic::{DMSRError, DMSRResult};
use crate::error::missing_value::MissingValueError;
use crate::event::event::{DataType, Field, JSONChangeEvent, Operation, Schema};
use crate::kafka::kafka::Kafka;
use async_trait::async_trait;
use bytes::Bytes;
use futures::{future, ready, Sink, StreamExt};
use std::collections::HashMap;
use std::pin::Pin;
use std::task::Poll;
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
        let client = match self.client.as_mut() {
            Some(client) => client,
            None => {
                let err = MissingValueError::new("Client is not initialized");
                return Err(err.into());
            }
        };

        let slot_name = format!(
            "slot_{}",
            &SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("Time went backwards")
                .as_secs()
        );

        let slot_query = format!(
            "CREATE_REPLICATION_SLOT {} TEMPORARY LOGICAL \"wal2json\"",
            slot_name
        );

        let slot_query_res: Vec<SimpleQueryRow> = client
            .simple_query(slot_query.as_str())
            .await?
            .into_iter()
            .filter_map(|m| match m {
                SimpleQueryMessage::Row(r) => Some(r),
                _ => None,
            })
            .collect();

        let lsn = match slot_query_res[0].get("consistent_point") {
            Some(lsn) => lsn,
            None => {
                return Err(DMSRError::PostgresError("No LSN found".into()));
            }
        };

        let query = format!(
            "START_REPLICATION SLOT {} LOGICAL {} (\"include-pk\" 'true', \"format-version\" '2')",
            slot_name, lsn
        );

        let duplex_stream = client.copy_both_simple::<Bytes>(&query).await?;

        let mut duplex_stream_pin = Box::pin(duplex_stream);

        loop {
            let event = match duplex_stream_pin.as_mut().next().await {
                Some(event_res) => event_res,
                None => break,
            }?;

            match event[0] {
                b'w' => {
                    let change_event = match self.parse_event(event) {
                        Ok(Some(change_event)) => change_event,
                        Ok(None) => continue,
                        Err(e) => {
                            println!("Error: {:?}", e);
                            continue;
                        }
                    };
                    println!("Parsed event: {:?}", change_event);
                    let topic = format!("{}-{}", self.topic_prefix, change_event.table);
                    println!("Topic: {:?}", topic);
                    kafka.ingest(topic, change_event, None).await?;
                }
                b'k' => {
                    Self::keep_alive(event, &mut duplex_stream_pin).await?;
                }
                _ => {
                    println!("Not recognized: {:?}", event);
                }
            }
        }

        Ok(())
    }
}

impl PostgresSourceConnector {
    fn parse_event(&self, event: Bytes) -> DMSRResult<Option<JSONChangeEvent>> {
        let b = &event[25..];
        let s = std::str::from_utf8(b)?;
        println!("Raw: {:?}", s);

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
        let columns = match raw_event.columns {
            Some(columns) => columns,
            None => return Ok(None),
        };

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

        let json_event = JSONChangeEvent {
            source_connector: self.connector_name.clone(),
            schema: Schema {
                r#type: DataType::Struct,
                fields,
            },
            payload,
            table: format!("{}.{}", schema, table),
            op: operation,
        };

        Ok(Some(json_event))
    }

    async fn keep_alive(
        event: Bytes,
        duplex_stream_pin: &mut Pin<Box<CopyBothDuplex<Bytes>>>,
    ) -> DMSRResult<()> {
        let last_byte = match event.last() {
            Some(last_byte) => last_byte,
            None => return Ok(()),
        };
        let timeout_imminent = last_byte == &1;
        if !timeout_imminent {
            return Ok(());
        }

        const SECONDS_FROM_UNIX_EPOCH_TO_2000: u128 = 946684800;
        let since_unix_epoch = SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros();
        let seconds_from_unix_to_2000 = SECONDS_FROM_UNIX_EPOCH_TO_2000 * 1000 * 1000;
        let time_since_2000: u64 = (since_unix_epoch - seconds_from_unix_to_2000).try_into()?;

        // see here for format details: https://www.postgresql.org/docs/10/protocol-replication.html
        let mut data_to_send: Vec<u8> = vec![];
        // Byte1('r'); Identifies the message as a receiver status update.
        data_to_send.extend_from_slice(&[114]); // "r" in ascii
                                                // The location of the last WAL byte + 1 received and written to disk in the standby.
        data_to_send.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]);
        // The location of the last WAL byte + 1 flushed to disk in the standby.
        data_to_send.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]);
        // The location of the last WAL byte + 1 applied in the standby.
        data_to_send.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]);
        // The client's system clock at the time of transmission, as microseconds since midnight on 2000-01-01.
        //0, 0, 0, 0, 0, 0, 0, 0,
        data_to_send.extend_from_slice(&time_since_2000.to_be_bytes());
        // Byte1; If 1, the client requests the server to reply to this message immediately. This can be used to ping the server, to test if the connection is still healthy.
        data_to_send.extend_from_slice(&[1]);

        let buf = Bytes::from(data_to_send);

        let mut next_step = 1;
        future::poll_fn(|cx| loop {
            match next_step {
                1 => {
                    ready!(duplex_stream_pin.as_mut().poll_ready(cx)).unwrap();
                }
                2 => {
                    duplex_stream_pin.as_mut().start_send(buf.clone()).unwrap();
                }
                3 => {
                    ready!(duplex_stream_pin.as_mut().poll_flush(cx)).unwrap();
                }
                4 => {
                    return Poll::Ready(());
                }
                _ => panic!(),
            }
            next_step += 1;
        })
        .await;

        Ok(())
    }
}
