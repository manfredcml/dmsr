use std::pin::Pin;
use crate::source::source::Source;
use crate::source::config::Config;
use std::task::Poll;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_postgres::{Client, NoTls, Row, SimpleQueryMessage, SimpleQueryRow, CopyBothDuplex};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{future, StreamExt, Sink, ready};

pub struct PostgresSource {
  endpoint: String,
  client: Option<Client>,
}

#[async_trait]
impl Source for PostgresSource {
  type Output = Row;

  fn new(config: Config) -> Self {
    PostgresSource {
      endpoint: config.endpoint,
      client: None,
    }
  }

  fn get_endpoint(&self) -> &String {
    &self.endpoint
  }

  async fn connect(&mut self) -> Result<()> {
    let (client, connection) = tokio_postgres::connect(
      self.endpoint.as_str(),
      NoTls,
    ).await?;

    tokio::spawn(async move {
      if let Err(e) = connection.await {
        eprintln!("connection error: {}", e);
      }
    });

    self.client = Some(client);
    Ok(())
  }

  async fn query(&mut self, query: String) -> Result<Vec<Row>> {
    let client = match self.client.as_mut() {
      Some(client) => client,
      None => return Err(anyhow!("Failed to connect to Postgres")),
    };

    let rows: Vec<Row> = client.query(query.as_str(), &[]).await?;
    return Ok(rows);
  }

  async fn stream(&mut self) -> Result<()> {
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

    let query = format!(
      "START_REPLICATION SLOT {} LOGICAL {}",
      slot_name,
      lsn
    );

    let duplex_stream = client
      .copy_both_simple::<Bytes>(&query)
      .await?;

    let mut duplex_stream_pin = Box::pin(duplex_stream);

    loop {
      let event = match duplex_stream_pin.as_mut().next().await {
        Some(event_res) => event_res,
        None => break,
      }?;

      match event[0] {
        b'w' => {
          println!("Got XLogData/data-change event: {:?}", event);
        }
        b'k' => {
          Self::keep_alive(event, &mut duplex_stream_pin).await?;
        }
        _ => {
          continue;
        }
      }
    }

    Ok(())
  }
}

impl PostgresSource {
  async fn keep_alive(event: Bytes,
                      duplex_stream_pin: &mut Pin<Box<CopyBothDuplex<Bytes>>>,
  ) -> Result<()> {
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
    future::poll_fn(|cx| {
      loop {
        match next_step {
          1 => { ready!(duplex_stream_pin.as_mut().poll_ready(cx)).unwrap(); }
          2 => { duplex_stream_pin.as_mut().start_send(buf.clone()).unwrap(); }
          3 => { ready!(duplex_stream_pin.as_mut().poll_flush(cx)).unwrap(); }
          4 => { return Poll::Ready(()); }
          _ => panic!(),
        }
        next_step += 1;
      }
    }).await;

    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[tokio::test]
  async fn it_works() {
    let conn_str = "host=localhost user=postgres password=postgres replication=database";
    let mut source = PostgresSource::new(Config {
      endpoint: conn_str.to_string(),
    });
    source.connect().await.expect("Failed to connect to Postgres");
    source.stream().await.expect("Failed to stream Postgres");
  }
}