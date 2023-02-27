use dms::sources_targets::source::Source;
use dms::sources_targets::source_config::Config;
use dms::sources_targets::postgres::Postgres;
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