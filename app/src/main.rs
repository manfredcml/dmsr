mod args;
mod config;
mod routes;
mod state;
mod yaml;

use crate::args::Args;
use crate::config::AppConfig;
use crate::routes::connector::{get_connectors, post_connectors};
use crate::routes::index::index;
use crate::state::AppState;
use actix_web::{web, App, HttpServer};
use clap::Parser;
use dms::error::generic::DMSRResult;
use dms::kafka::kafka_config::KafkaConfig;
use dms::kafka::kafka_impl::Kafka;
use log::info;
use std::fs::File;

#[tokio::main]
async fn main() -> DMSRResult<()> {
    env_logger::init();

    let args = Args::parse();

    let config_file = File::open(args.config_path)?;
    let config: AppConfig = serde_yaml::from_reader(config_file)?;
    println!("App Config: {:?}", config);

    let kafka = init_kafka().await?;
    let app_state = web::Data::new(AppState { kafka });

    HttpServer::new(move || {
        App::new()
            .app_data(app_state.clone())
            .service(index)
            .service(get_connectors)
            .service(post_connectors)
    })
    .bind("127.0.0.1:8000")?
    .run()
    .await?;

    Ok(())
}

async fn init_kafka() -> DMSRResult<Kafka> {
    let kafka_config = KafkaConfig {
        bootstrap_servers: "localhost:9092".to_string(),
    };
    let mut kafka = Kafka::new(&kafka_config)?;

    info!("Connecting to Kafka...");
    kafka.connect().await?;

    info!("Creating default topic...");
    kafka.create_config_topic().await?;

    Ok(kafka)
}
