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
use dms::error::generic::{DMSRError, DMSRResult};
use dms::kafka::kafka_impl::Kafka;
use log::info;
use rdkafka::consumer::Consumer;
use rdkafka::Message;
use std::fs::File;

#[tokio::main]
async fn main() -> DMSRResult<()> {
    env_logger::init();

    let args = Args::parse();

    let config_file = File::open(args.config_path)?;
    let config: AppConfig = serde_yaml::from_reader(config_file)?;
    println!("App Config: {:?}", config);

    let kafka = init_kafka(&config).await?;
    let app_state = web::Data::new(AppState { kafka });

    tokio::spawn(async move {
        subscribe_to_config_topic(&config).await.unwrap();
    });

    info!("Starting HTTP server...");
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

async fn init_kafka(config: &AppConfig) -> DMSRResult<Kafka> {
    let mut kafka = Kafka::new(&config.kafka)?;

    info!("Connecting to Kafka...");
    kafka.connect().await?;

    info!("Creating default topic...");
    kafka.create_config_topic().await?;

    Ok(kafka)
}

async fn subscribe_to_config_topic(config: &AppConfig) -> DMSRResult<()> {
    let config_topic = &config.kafka.config_topic;

    let mut kafka = Kafka::new(&config.kafka)?;
    kafka.connect().await?;

    let consumer = match kafka.consumer {
        Some(ref consumer) => consumer,
        None => {
            let err = dms::error::missing_value::MissingValueError {
                field_name: "admin",
            };
            return Err(DMSRError::from(err));
        }
    };

    consumer.subscribe(&[config_topic])?;

    loop {
        match consumer.recv().await {
            Ok(msg) => {
                let key = msg.key().unwrap_or_default();
                let payload = msg.payload().unwrap_or_default();
                println!("Received message: {:?} {:?}", key, payload);
            }
            Err(e) => {
                println!("Kafka error: {}", e);
            }
        }
    }
}
