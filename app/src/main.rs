mod args;
mod config;
mod yaml;

use args::Args;
use clap::Parser;
use futures::lock::Mutex;
use log::info;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    info!("Starting up...");

    let args = Args::parse();
    let config = yaml::load_config(&args.config_path)?;
    config.validate()?;

    info!("Config: {:?}", config);

    let mut queue = config.queue.get_streamer()?;
    queue.connect().await?;
    info!("connected!!");

    let queue = Arc::new(Mutex::new(queue));

    // Start sources_targets
    let mut sources: Vec<Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>> = Vec::new();

    for sc in config.sources {
        let mut source = sc.get_source()?;

        let q = Arc::clone(&queue);
        let source_future = async move {
            source.connect().await?;
            source.stream(q).await?;
            Ok(())
        };

        sources.push(Box::pin(source_future));
    }

    futures::future::try_join_all(sources.into_iter().map(tokio::spawn)).await?;

    info!("Terminating...");
    Ok(())
}
