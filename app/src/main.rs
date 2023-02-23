mod args;

use anyhow::Result;
use args::Args;
use clap::Parser;
use dms::events::event_stream::DataStream;
use dms::sources::config::Config;
use dms::sources::postgres::PostgresSource;
use dms::sources::source::Source;
use log::{error, info};

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    info!("Starting up...");

    let args = Args::parse();
    let source = args.source.as_str();

    match source {
        "postgres" => {
            let mut source = PostgresSource::new(Config {
        endpoint: "host=localhost user=postgres password=postgres dbname=postgres replication=database".to_string(),
      });
            source.connect().await?;

            let mut event_stream = DataStream::new();

            let tx_future = source.stream(&mut event_stream.tx);
            let rx_future = async {
                loop {
                    let event = event_stream.rx.recv().await;
                    println!("Here you go - {:?}", event);
                }
            };

            let (_, _) = tokio::join!(tx_future, rx_future);
        }
        _ => {
            error!("Unknown source: {}", source);
        }
    }

    info!("Terminating...");
    Ok(())
}
