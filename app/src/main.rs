mod args;

use args::Args;
use clap::Parser;
use log::{error, info};
use dms::sources::config::Config;
use dms::sources::postgres::PostgresSource;
use dms::sources::source::Source;
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
  env_logger::init();

  info!("Starting up...");

  let args = Args::parse();
  let source = args.source.as_str();

  match source {
    "postgres" => {
      let mut source = PostgresSource::new(Config {
        endpoint: "host=localhost user=postgres password=postgres dbname=postgres".to_string(),
      });
      source.connect().await?;
      source.stream().await?;
    }
    _ => {
      error!("Unknown source: {}", source);
    }
  }

  info!("Terminating...");
  Ok(())
}
