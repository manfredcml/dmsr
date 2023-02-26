mod args;
mod config;
mod yaml;

use args::Args;
use clap::Parser;
use dms::events::event_stream::DataStream;
use log::{error, info};
use std::future::Future;
use std::pin::Pin;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    info!("Starting up...");

    let args = Args::parse();
    let config = yaml::load_config(&args.config_path)?;

    info!("Config: {:?}", config);

    // Start streamer
    let mut streamer = config.queue.get_streamer()?;
    streamer.connect().await?;
    streamer.ingest(vec![]).await?;

    info!("connected!!");

    // Start sources
    let mut sources: Vec<Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>> = Vec::new();

    for sc in config.sources {
        let source_type = sc.source_type.clone();
        let mut source = match sc.get_source() {
            Some(s) => s,
            None => {
                error!("Unknown source type: {:?}", source_type);
                continue;
            }
        };

        let source_future = async move {
            source.connect().await?;
            let mut event_stream = DataStream::new();
            let tx_future = source.stream(&mut event_stream.tx);
            let rx_future = async {
                loop {
                    let event = match event_stream.rx.recv().await {
                        Some(e) => e,
                        None => continue,
                    };

                    // Send event into Kafka

                    println!("Events : {:?}", event);
                }
            };
            let _ = tokio::join!(tx_future, rx_future);
            Ok(())
        };

        sources.push(Box::pin(source_future));
    }

    futures::future::try_join_all(sources.into_iter().map(tokio::spawn)).await?;

    info!("Terminating...");
    Ok(())
}
