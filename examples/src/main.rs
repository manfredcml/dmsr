use dms::source::source::Source;
use dms::source::config::Config;
use dms::source::postgres::PostgresSource;
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