mod args;

use args::Args;
use clap::Parser;
use log::{error, info};

#[tokio::main]
async fn main() {
  env_logger::init();

  info!("Starting up...");

  let args = Args::parse();
  let source = args.source.as_str();

  match source {
    "postgres" => {
      info!("Starting Postgres source...");
    }
    _ => {
      error!("Unknown source: {}", source);
    }
  }

  info!("Terminating...")
}
