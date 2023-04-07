use crate::connector::connector::Connector;
use crate::connector::postgres_source::config::PostgresSourceConfig;
use crate::connector::postgres_source::event::{Action, Column, RawPostgresEvent};
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
            "CREATE_REPLICATION_SLOT {} TEMPORARY LOGICAL \"pgoutput\"",
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

        let query = format!("START_REPLICATION SLOT {} LOGICAL {} (\"proto_version\" '1', \"publication_names\" 'all_tables')", slot_name, lsn);

        let stream = client.copy_both_simple::<Bytes>(&query).await?;
        let mut stream = Box::pin(stream);

        while let Some(event) = stream.next().await {
            let event = event?;
            let first_byte = &event[0];

            match first_byte {
                b'k' => {
                    self.keep_alive(&event, &mut stream).await?;
                }
                b'w' => {
                    println!("Event: {:?}", event);

                    let mut cursor = Cursor::new(&event);

                    let message_type = cursor.read_u8()?;
                    let transaction_id = cursor.read_u64::<BigEndian>()?;
                    let relation_id = cursor.read_u32::<BigEndian>()?;

                    println!("Message type: {:?}", message_type);
                    println!("Transaction ID: {:?}", transaction_id);
                    println!("Relation ID: {:?}", relation_id);

                    let tuple_data = &event[cursor.position() as usize..];
                    println!("Tuple data: {:?}", tuple_data);


                    // let tuple_data = &event[cursor.position() as usize..];

                    // let topic = format!("{}-{}", self.topic_prefix, change_event.table);
                    // println!("Topic: {:?}", topic);
                    // kafka.ingest(topic, change_event, None).await?;
                    println!("====================================")
                }
                _ => {}
            }
        }

        //
        // let mut duplex_stream_pin = Box::pin(duplex_stream);
        //
        // loop {
        //     let event = match duplex_stream_pin.as_mut().next().await {
        //         Some(event_res) => event_res,
        //         None => break,
        //     }?;
        //
        //     match event[0] {
        //         b'w' => {
        //             let change_event = match self.parse_event(event) {
        //                 Ok(Some(change_event)) => change_event,
        //                 Ok(None) => continue,
        //                 Err(e) => {
        //                     println!("Error: {:?}", e);
        //                     continue;
        //                 }
        //             };
        //             println!("Parsed event: {:?}", change_event);
        //             let topic = format!("{}-{}", self.topic_prefix, change_event.table);
        //             println!("Topic: {:?}", topic);
        //             kafka.ingest(topic, change_event, None).await?;
        //         }
        //         b'k' => {
        //             Self::keep_alive(event, &mut duplex_stream_pin).await?;
        //         }
        //         _ => {
        //             println!("Not recognized: {:?}", event);
        //         }
        //     }
        // }

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

    async fn keep_alive(
        &self,
        event: &Bytes,
        stream: &mut Pin<Box<CopyBothDuplex<Bytes>>>,
    ) -> DMSRResult<()> {
        let last_byte = event.last().unwrap_or(&0);
        if last_byte != &1 {
            return Ok(()); // We don't need to send a reply
        }

        let now = SystemTime::now();
        let duration_since_epoch = now.duration_since(UNIX_EPOCH)?;
        let timestamp_micros = duration_since_epoch.as_secs() * 1_000_000
            + u64::from(duration_since_epoch.subsec_micros());

        // Write the Standby Status Update message header
        let mut buf = Cursor::new(Vec::new());
        buf.write_u8(b'r')?;
        buf.write_i64::<BigEndian>(0)?; // Write your current XLog position here
        buf.write_i64::<BigEndian>(0)?; // Write your current flush position here
        buf.write_i64::<BigEndian>(0)?; // Write your current apply position here
        buf.write_i64::<BigEndian>(timestamp_micros as i64)?; // Write the current timestamp here
        buf.write_u8(1)?; // 1 if you want to request a reply from the server, 0 otherwise

        let msg = buf.into_inner();
        let msg = Bytes::from(msg);

        future::poll_fn(|cx| {
            ready!(stream.as_mut().poll_ready(cx))?;
            stream.as_mut().start_send(msg.clone())?;
            stream.as_mut().poll_flush(cx)
        })
        .await?;

        Ok(())
    }
}
