use dms::database::source::Source;
use dms::database::source_config::Config;
use dms::database::postgres::Postgres;
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
  let conn_str = "host=localhost user=postgres password=postgres replication=database";
  let mut source = Postgres::new(Config {
    endpoint: conn_str.to_string(),
  });
  source.connect().await?;
  source.stream().await?;
  Ok(())
}