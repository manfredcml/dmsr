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
use dms::connector::config::ConnectorConfig;
use dms::connector::connector::Connector;
use dms::connector::kind::ConnectorKind;
use dms::connector::postgres_sink::config::PostgresSinkConfig;
use dms::connector::postgres_sink::connector::PostgresSinkConnector;
use dms::connector::postgres_source::config::PostgresSourceConfig;
use dms::connector::postgres_source::connector::PostgresSourceConnector;
use dms::error::generic::{DMSRError, DMSRResult};
use dms::kafka::kafka::Kafka;
use log::{error, info};
use rdkafka::consumer::Consumer;
use rdkafka::message::BorrowedMessage;
use rdkafka::Message;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;
use dms::connector::mysql_source::config::MySQLSourceConfig;
use dms::connector::mysql_source::connector::MySQLSourceConnector;

#[tokio::main]
async fn main() -> DMSRResult<()> {
    init_log();

    let args = Args::parse();

    let config_file = File::open(args.config_path)?;
    let config: AppConfig = serde_yaml::from_reader(config_file)?;
    info!("App Config: {:?}", config);

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

    let active_connectors: HashMap<String, JoinHandle<DMSRResult<()>>> = HashMap::new();
    let active_connectors = Arc::new(Mutex::new(active_connectors));

    loop {
        match consumer.recv().await {
            Ok(msg) => {
                let c = Arc::clone(&active_connectors);
                let result = parse_config_topic_message(config, &msg, c);
                match result {
                    Ok(_) => {
                        info!("Message parsed successfully");
                    }
                    Err(e) => {
                        error!("{:?}", e);
                    }
                }
            }
            Err(e) => {
                error!("Kafka error: {}", e);
            }
        }
    }
}

fn parse_config_topic_message(
    config: &AppConfig,
    msg: &BorrowedMessage,
    active_connectors: Arc<Mutex<HashMap<String, JoinHandle<DMSRResult<()>>>>>,
) -> DMSRResult<()> {
    let key = msg.key().unwrap_or_default();
    let payload = msg.payload().unwrap_or_default();

    let connector_name = String::from_utf8(key.to_vec())?;

    let payload = String::from_utf8(payload.to_vec())?;
    let payload: ConnectorConfig = serde_json::from_str(&payload)?;

    let connector_config = payload.config.clone();

    match payload.connector_type {
        ConnectorKind::PostgresSource => {
            let mut kafka = Kafka::new(&config.kafka)?;
            let config: PostgresSourceConfig = serde_json::from_value(connector_config)?;
            let connector_name_clone = connector_name.clone();
            let handle: JoinHandle<DMSRResult<()>> = tokio::spawn(async move {
                let mut connector = PostgresSourceConnector::new(
                    connector_name_clone,
                    payload.topic_prefix,
                    &config,
                )?;
                info!("Connecting to Kafka...");
                kafka.connect().await?;
                info!("Connecting to Postgres...");
                connector.connect().await?;
                info!("Starting stream...");
                let result = connector.stream(kafka).await;
                if let Err(e) = result {
                    error!("Error streaming {:?}", e);
                }
                Ok(())
            });
            let mut active_connectors = active_connectors.lock().unwrap();
            active_connectors.insert(connector_name, handle);
        }
        ConnectorKind::PostgresSink => {
            // let mut kafka = Kafka::new(&config.kafka)?;
            // let config: PostgresSinkConfig = serde_json::from_value(connector_config)?;
            // let connector_name_clone = connector_name.clone();
            // let handle: JoinHandle<DMSRResult<()>> = tokio::spawn(async move {
            //     let mut connector = PostgresSinkConnector::new(
            //         connector_name_clone,
            //         payload.topic_prefix,
            //         &config,
            //     )?;
            //     info!("Connecting to Kafka...");
            //     kafka.connect().await?;
            //     info!("Connecting to Postgres...");
            //     connector.connect().await?;
            //     info!("Starting stream...");
            //     let result = connector.stream(kafka).await;
            //     if let Err(e) = result {
            //         error!("Error streaming {:?}", e);
            //     }
            //     Ok(())
            // });
            // let mut active_connectors = active_connectors.lock().unwrap();
            // active_connectors.insert(connector_name, handle);
        }
        ConnectorKind::MySQLSource => {
            // let mut kafka = Kafka::new(&config.kafka)?;
            // let config: MySQLSourceConfig = serde_json::from_value(connector_config)?;
            // let connector_name_clone = connector_name.clone();
            // let handle: JoinHandle<DMSRResult<()>> = tokio::spawn(async move {
            //     let mut connector = MySQLSourceConnector::new(
            //         connector_name_clone,
            //         payload.topic_prefix,
            //         &config,
            //     )?;
            //     info!("Connecting to Kafka...");
            //     kafka.connect().await?;
            //     info!("Connecting to Postgres...");
            //     connector.connect().await?;
            //     info!("Starting stream...");
            //     let result = connector.stream(kafka).await;
            //     if let Err(e) = result {
            //         error!("Error streaming {:?}", e);
            //     }
            //     Ok(())
            // });
            // let mut active_connectors = active_connectors.lock().unwrap();
            // active_connectors.insert(connector_name, handle);
        }
    }

    Ok(())
}
