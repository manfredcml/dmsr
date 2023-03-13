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
use dms::connector::connector::Connector;
use dms::connector::kind::ConnectorKind;
use dms::connector::postgres_source::config::PostgresSourceConfig;
use dms::connector::postgres_source::connector::PostgresSourceConnector;
use dms::error::generic::{DMSRError, DMSRResult};
use dms::kafka::kafka::Kafka;
use log::info;
use rdkafka::consumer::Consumer;
use rdkafka::message::BorrowedMessage;
use rdkafka::Message;
use serde_json::Value;
use std::fs::File;
use std::io::Write;

#[tokio::main]
async fn main() -> DMSRResult<()> {
    init_log();

    let args = Args::parse();

    let config_file = File::open(args.config_path)?;
    let config: AppConfig = serde_yaml::from_reader(config_file)?;
    println!("App Config: {:?}", config);

    let kafka = init_kafka(&config).await?;
    let app_state = web::Data::new(AppState {
        kafka,
        app_config: config.clone(),
    });

    let config_clone = config.clone();
    tokio::spawn(async move {
        subscribe_to_config_topic(&config_clone).await.unwrap();
    });

    info!("Starting HTTP server...");
    HttpServer::new(move || {
        App::new()
            .app_data(app_state.clone())
            .service(index)
            .service(get_connectors)
            .service(post_connectors)
    })
    .bind(format!("127.0.0.1:{}", config.port))?
    .run()
    .await?;

    Ok(())
}

fn init_log() {
    env_logger::builder()
        .format(|buf, record| {
            let ts = buf.timestamp_micros();
            writeln!(
                buf,
                "{}: {:?}: {}: {}",
                ts,
                std::thread::current().id(),
                buf.default_level_style(record.level())
                    .value(record.level()),
                record.args()
            )
        })
        .init();
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
                let result = parse_config_topic_message(&msg);
                match result {
                    Ok(_) => {
                        info!("Message parsed successfully");
                    }
                    Err(e) => {
                        info!("Error parsing message: {:?}", e);
                    }
                }
            }
            Err(e) => {
                println!("Kafka error: {}", e);
            }
        }
    }
}

fn parse_config_topic_message(msg: &BorrowedMessage) -> DMSRResult<()> {
    let key = msg.key().unwrap_or_default();
    let payload = msg.payload().unwrap_or_default();

    let connector_name = String::from_utf8(key.to_vec())?;

    let connector_config = String::from_utf8(payload.to_vec())?;
    let connector_config: Value = serde_json::from_str(&connector_config)?;

    let connector_type = connector_config["connector_type"]
        .as_str()
        .unwrap_or_default();
    let connector_type: ConnectorKind = connector_type.parse()?;

    if let Value::Object(map) = connector_config {
        let mut new_map = serde_json::Map::new();
        for (key, value) in map {
            if key != "connector_type" {
                new_map.insert(key, value);
            }
        }
        info!("New map: {:?}", new_map);
        info!("Connector type: {:?}", connector_type);
        match connector_type {
            ConnectorKind::PostgresSource => {
                let config: PostgresSourceConfig = serde_json::from_value(Value::Object(new_map))?;
                info!("Parsed config: {:?}", config);
                tokio::spawn(async move {
                    let mut connector = PostgresSourceConnector::new(&config).unwrap();
                    connector.connect().await.unwrap();
                    info!("Connected to Postgres");
                });
            }
            ConnectorKind::PostgresSink => {}
        }
    }

    Ok(())
}
