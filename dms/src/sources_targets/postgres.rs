use crate::events::event::ChangeEvent;
use crate::events::postgres_event::PostgresEvent;
use crate::events::raw_postgres_event::RawPostgresEvent;
use crate::queue::queue::Queue;
use crate::sources_targets::source::Source;
use crate::sources_targets::source_config::SourceConfig;
use crate::sources_targets::source_kind::SourceKind;
use crate::sources_targets::target::Target;
use crate::sources_targets::target_config::TargetConfig;
use crate::sources_targets::target_kind::TargetKind;
use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use futures::lock::Mutex;
use futures::{future, ready, Sink, StreamExt};
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_postgres::{Client, CopyBothDuplex, NoTls, SimpleQueryMessage, SimpleQueryRow};

pub struct Postgres {
    source_config: Option<SourceConfig>,
    target_config: Option<TargetConfig>,
    client: Option<Client>,
}

impl Postgres {
    fn parse_event(&self, event: Bytes) -> anyhow::Result<Vec<ChangeEvent>> {
        let b = &event[25..];
        let s = std::str::from_utf8(b)?;
        println!("{}", s);

        let raw_event: RawPostgresEvent = serde_json::from_str(s)?;

        let change_events: Vec<_> = raw_event
            .change
            .into_iter()
            .filter_map(|c| {
                let postgres_event = PostgresEvent {
                    schema: c.schema,
                    table: c.table,
                    column_names: c.column_names,
                    column_types: c.column_types,
                    column_values: c.column_values,
                };

                let event_kind = match c.kind.as_str().parse() {
                    Ok(event_kind) => event_kind,
                    Err(_) => return None,
                };

                let source_name = match self.get_source_name() {
                    Ok(source_name) => source_name,
                    Err(_) => return None,
                };

                Some(ChangeEvent {
                    source_name: source_name.clone(),
                    source_kind: SourceKind::Postgres,
                    event_kind,
                    postgres_event: Some(postgres_event),
                })
            })
            .collect();

        Ok(change_events)
    }

    async fn keep_alive(
        event: Bytes,
        duplex_stream_pin: &mut Pin<Box<CopyBothDuplex<Bytes>>>,
    ) -> anyhow::Result<()> {
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
        let seconds_from_unix_to_2000 = (SECONDS_FROM_UNIX_EPOCH_TO_2000 * 1000 * 1000) as u128;
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

#[async_trait]
impl Source for Postgres {
    fn new(config: &SourceConfig) -> anyhow::Result<Box<Self>> {
        if config.kind != SourceKind::Postgres {
            return Err(anyhow!("Invalid source type for PostgresSource"));
        }

        if config.postgres_config == None {
            return Err(anyhow!("Invalid config for PostgresSource"));
        }

        Ok(Box::new(Postgres {
            source_config: Some(config.clone()),
            target_config: None,
            client: None,
        }))
    }

    fn get_source_name(&self) -> anyhow::Result<&String> {
        match &self.source_config {
            Some(s) => Ok(&s.name),
            None => Err(anyhow!("Missing source name")),
        }
    }

    fn get_source_config(&self) -> anyhow::Result<&SourceConfig> {
        match &self.source_config {
            Some(s) => Ok(s),
            None => Err(anyhow!("Missing source config")),
        }
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        let source_config = self.get_source_config()?;
        let postgres_config = match &source_config.postgres_config {
            Some(c) => c,
            None => return Err(anyhow!("Missing postgres config")),
        };

        let endpoint = format!(
            "host={} port={} user={} password={} replication=database",
            postgres_config.host,
            postgres_config.port,
            postgres_config.user,
            postgres_config.password
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

    async fn stream(&mut self, queue: Arc<Mutex<Box<dyn Queue + Send>>>) -> anyhow::Result<()> {
        let client = match self.client.as_mut() {
            Some(client) => client,
            None => return Err(anyhow!("Failed to connect to Postgres")),
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
            None => return Err(anyhow!("Failed to get LSN")),
        };

        let query = format!("START_REPLICATION SLOT {} LOGICAL {}", slot_name, lsn);

        let duplex_stream = client.copy_both_simple::<Bytes>(&query).await?;

        let mut duplex_stream_pin = Box::pin(duplex_stream);

        loop {
            let event = match duplex_stream_pin.as_mut().next().await {
                Some(event_res) => event_res,
                None => break,
            }?;

            match event[0] {
                b'w' => {
                    let change_events = Self::parse_event(&self, event)?;
                    let mut q = queue.lock().await;
                    for e in change_events {
                        println!("Ingesting: {:?}", e);
                        q.ingest(e).await?;
                    }
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

#[async_trait]
impl Target for Postgres {
    fn new(config: &TargetConfig) -> anyhow::Result<Box<Self>> {
        if config.kind != TargetKind::Postgres {
            return Err(anyhow!("Invalid source type for PostgresSource"));
        }

        if config.postgres_config == None {
            return Err(anyhow!("Invalid config for PostgresSource"));
        }

        Ok(Box::new(Postgres {
            source_config: None,
            target_config: Some(config.clone()),
            client: None,
        }))
    }

    fn get_target_name(&self) -> anyhow::Result<&String> {
        match &self.target_config {
            Some(target_config) => Ok(&target_config.name),
            None => Err(anyhow!("Missing target name")),
        }
    }

    fn get_target_config(&self) -> anyhow::Result<&TargetConfig> {
        match &self.target_config {
            Some(s) => Ok(s),
            None => Err(anyhow!("Missing target config")),
        }
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        let config = match &self.get_target_config()?.postgres_config {
            Some(c) => c,
            None => return Err(anyhow!("Missing postgres config")),
        };

        let endpoint = format!(
            "host={} port={} user={} password={} replication=database",
            config.host, config.port, config.user, config.password
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

    async fn accept(&mut self, data: ChangeEvent) -> anyhow::Result<()> {
        // let client = self.client.as_mut().unwrap();
        // let postgres_event = data.postgres_event.as_ref().unwrap();
        // let query = postgres_event.query.as_str();
        // let params = &postgres_event.params;
        // let result = client.execute(query, params).await?;
        // println!("result: {}", result);
        Ok(())
    }
}
