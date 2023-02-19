use std::time::SystemTime;
use crate::source::source::Source;
use tokio_postgres::{Client, NoTls, Socket, Row, SimpleQueryMessage, SimpleQueryRow};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use futures::{future, StreamExt, Sink, ready};
use crate::source::config::Config;

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
}

impl PostgresSource {
  pub async fn stream(&mut self) -> Result<()> {
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
      .copy_both_simple::<bytes::Bytes>(&query)
      .await?;

    let mut duplex_stream_pin = Box::pin(duplex_stream);
    
    loop {
      let event_res_opt = match duplex_stream_pin.as_mut().next().await {
        Some(event_res) => event_res,
        None => break,
      }?;
      println!("{:?}", event_res_opt)
    }

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
    // let _: Vec<Row> = source
    //   .query("SELECT * FROM public.test_tbl".to_string())
    //   .await
    //   .expect("Failed to query Postgres");
  }
}