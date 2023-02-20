use dms::sources::source::Source;
use dms::sources::config::Config;
use dms::sources::postgres::PostgresSource;
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
  let conn_str = "host=localhost user=postgres password=postgres replication=database";
  let mut source = PostgresSource::new(Config {
    endpoint: conn_str.to_string(),
  });
  source.connect().await?;
  source.stream().await?;
  Ok(())
}