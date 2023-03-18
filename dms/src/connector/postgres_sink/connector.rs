use crate::connector::connector::Connector;
use crate::connector::postgres_sink::config::PostgresSinkConfig;
use crate::error::generic::{DMSRError, DMSRResult};
use crate::kafka::kafka::Kafka;
use async_trait::async_trait;
use rdkafka::consumer::Consumer;
use tokio_postgres::NoTls;

pub struct PostgresSinkConnector {
    config: PostgresSinkConfig,
    client: Option<tokio_postgres::Client>,
    connector_name: String,
    topic_prefix: String,
}

#[async_trait]
impl Connector for PostgresSinkConnector {
    type Config = PostgresSinkConfig;

    fn new(
        connector_name: String,
        topic_prefix: String,
        config: &PostgresSinkConfig,
    ) -> DMSRResult<Box<Self>> {
        Ok(Box::new(PostgresSinkConnector {
            config: config.clone(),
            client: None,
            connector_name,
            topic_prefix,
        }))
    }

    async fn connect(&mut self) -> DMSRResult<()> {
        let endpoint = format!(
            "host={} port={} user={} password={}",
            self.config.host, self.config.port, self.config.user, self.config.password
        );

        let (client, connection) = tokio_postgres::connect(endpoint.as_str(), NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        self.client = Some(client);
        Ok(())
    }

    async fn stream(&mut self, mut kafka: Kafka) -> DMSRResult<()> {
        let consumer = match &mut kafka.consumer {
            Some(consumer) => consumer,
            None => {
                return Err(DMSRError::MissingValueError(
                    "Kafka consumer not initialized".to_string(),
                ))
            }
        };
        let topics_to_subscribe: Vec<String> = self
            .config
            .tables
            .iter()
            .map(|table| format!("{}-{}", self.topic_prefix, table))
            .collect();
        let topics_to_subscribe: Vec<&str> = topics_to_subscribe.iter().map(|s| &**s).collect();
        let topics_to_subscribe = topics_to_subscribe.as_slice();
        consumer.subscribe(topics_to_subscribe)?;

        loop {
            match consumer.recv().await {
                Ok(msg) => {
                    println!("Received message: {:?}", msg);
                }
                Err(e) => {
                    eprintln!("Error while receiving message: {:?}", e);
                }
            }
        }
    }
}
