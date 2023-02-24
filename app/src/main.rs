mod args;
mod config;
mod yaml;

use args::Args;
use clap::Parser;
use dms::events::event_stream::DataStream;
use log::{error, info};
use std::future::Future;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    info!("Starting up...");

    let args = Args::parse();
    let config = yaml::load_config(&args.config_path)?;

    info!("Config: {:?}", config);

    let mut sources: Vec<_> = vec![];
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
            source.connect().await.unwrap();
            let mut event_stream = DataStream::new();
            let tx_future = source.stream(&mut event_stream.tx);
            let rx_future = async {
                loop {
                    let event = event_stream.rx.recv().await;
                    println!("Here you go - {:?}", event);
                }
            };

            let _ = tokio::join!(tx_future, rx_future);
        };
        sources.push(source_future);
    }

    futures::future::join_all(sources).await;

    info!("Terminating...");
    Ok(())
}
