mod args;
mod logger;
mod routes;
mod state;

use crate::args::Args;
use crate::logger::init_log;
use crate::routes::connector::{get_connectors, post_connectors};
use crate::routes::index::index;
use crate::state::{AppState, MutexActiveConnectors};
use actix_web::{web, App, HttpServer};
use clap::Parser;
use dms::connector::config::ConnectorConfig;
use dms::connector::connector::SourceConnector;
use dms::connector::kind::ConnectorKind;
use dms::connector::mysql_source::config::MySQLSourceConfig;
use dms::connector::mysql_source::connector::MySQLSourceConnector;
use dms::connector::postgres_sink::config::PostgresSinkConfig;
// use dms::connector::postgres_sink::connector::PostgresSinkConnector;
use dms::connector::postgres_source::config::PostgresSourceConfig;
// use dms::connector::postgres_source::connector::PostgresSourceConnector;
use dms::error::error::{DMSRError, DMSRResult};
use dms::kafka::config::KafkaConfig;
use dms::kafka::kafka::Kafka;
use log::info;
use rdkafka::consumer::Consumer;
use rdkafka::Message;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;

#[tokio::main]
async fn main() -> DMSRResult<()> {
    init_log();

    let args = Args::parse();
    let kafka_config = args.to_kafka_config();

    // Create config storage topic
    let mut kafka = Kafka::new(&kafka_config).await?;
    kafka.create_config_topic().await?;

    let active_connectors: MutexActiveConnectors = Arc::new(Mutex::new(HashMap::new()));
    let active_connectors_clone = Arc::clone(&active_connectors);

    tokio::spawn(async move {
        let kafka = Kafka::new(&kafka_config).await?;
        subscribe_to_config_topic(kafka, active_connectors_clone).await?;
        Ok::<(), DMSRError>(())
    });

    let app_state = web::Data::new(AppState {
        kafka,
        active_connectors,
    });

    info!("Starting HTTP server...");
    HttpServer::new(move || {
        let app_state = app_state.clone();
        App::new()
            .app_data(app_state)
            .service(index)
            .service(get_connectors)
            .service(post_connectors)
    })
    .bind(format!("127.0.0.1:{}", args.port))?
    .run()
    .await?;

    Ok(())
}

async fn subscribe_to_config_topic(
    kafka: Kafka,
    active_connectors: MutexActiveConnectors,
) -> DMSRResult<()> {
    let config_topic = kafka.config.config_topic.as_str();
    kafka.consumer.subscribe(&[config_topic])?;

    while let Ok(msg) = kafka.consumer.recv().await {
        let key = msg.key().unwrap_or_default();
        let payload = msg.payload().unwrap_or_default();
        let payload = String::from_utf8(payload.to_vec())?;

        let connector_name = String::from_utf8(key.to_vec())?;
        println!("Connector name: {}", connector_name);
        let connector_config: ConnectorConfig = serde_json::from_str(&payload)?;
        let connector_type = connector_config.connector_type;
        let connector_config = connector_config.config;

        let active_connectors = Arc::clone(&active_connectors);

        match connector_type {
            ConnectorKind::PostgresSource => {
                // start_connector::<PostgresSourceConfig, PostgresSourceConnector>(
                //     &kafka.config,
                //     connector_name,
                //     connector_config,
                //     active_connectors,
                // )
                // .await?;
            }
            ConnectorKind::PostgresSink => {
                // start_connector::<PostgresSinkConfig, PostgresSinkConnector>(
                //     &kafka.config,
                //     connector_name,
                //     connector_config,
                //     active_connectors,
                // )
                // .await?;
            }
            ConnectorKind::MySQLSource => {
                start_connector::<MySQLSourceConfig, MySQLSourceConnector>(
                    &kafka.config,
                    connector_name,
                    connector_config,
                    active_connectors,
                )
                .await?;
            }
        }
    }

    Ok(())
}

async fn start_connector<ConfigType, ConnectorType>(
    kafka_config: &KafkaConfig,
    connector_name: String,
    connector_config: serde_json::Value,
    active_connectors: MutexActiveConnectors,
) -> DMSRResult<()>
where
    ConfigType: serde::de::DeserializeOwned + Send + Sync,
    ConnectorType: SourceConnector<Config = ConfigType> + Send + Sync,
{
    let kafka_config = kafka_config.clone();
    let connector_name_clone = connector_name.clone();

    let handle: JoinHandle<DMSRResult<()>> = tokio::spawn(async move {
        let config: ConfigType = serde_json::from_value(connector_config)?;

        info!("Connecting to Kafka...");
        let kafka = Kafka::new(&kafka_config).await?;

        info!("Connecting to data source...");
        let mut connector = ConnectorType::new(connector_name_clone, config).await?;

        info!("Preparing stream...");
        let mut stream = connector.make_kafka_message_stream().await?;
        connector.send_to_kafka(&kafka, &mut stream).await?;

        info!("Stream stopped");

        Ok(())
    });

    let mut active_connectors = active_connectors
        .lock()
        .map_err(|_| DMSRError::LockError("active_connectors".into()))?;

    active_connectors.insert(connector_name, handle);

    Ok(())
}
