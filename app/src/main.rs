mod args;
mod config;
mod yaml;

use actix_web::{get, App, HttpResponse, HttpServer};
use args::Args;
use clap::Parser;
use futures::lock::Mutex;
use log::info;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

#[get("/")]
pub async fn index() -> HttpResponse {
    HttpResponse::Ok().body("Hello world!")
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    HttpServer::new(|| App::new().service(index))
        .bind("127.0.0.1:8000")?
        .run()
        .await
}

// #[tokio::main]
// async fn main() -> anyhow::Result<()> {
//     env_logger::init();
//
//     info!("Starting up...");
//
//     let args = Args::parse();
//     let config = yaml::load_config(&args.config_path)?;
//     config.validate()?;
//
//     info!("Config: {:?}", config);
//
//     let mut kafka = config.kafka.get_streamer()?;
//     kafka.connect().await?;
//     info!("connected!!");
//
//     let kafka = Arc::new(Mutex::new(kafka));
//
//     // Start database
//     let mut sources: Vec<Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>> = Vec::new();
//
//     for sc in config.sources {
//         let mut source = sc.get_source()?;
//
//         let q = Arc::clone(&kafka);
//         let source_future = async move {
//             source.connect().await?;
//             source.stream(q).await?;
//             Ok(())
//         };
//
//         sources.push(Box::pin(source_future));
//     }
//
//     futures::future::try_join_all(sources.into_iter().map(tokio::spawn)).await?;
//
//     info!("Terminating...");
//     Ok(())
// }
