use crate::error::error::DMSRResult;
use crate::kafka::config::KafkaConfig;
use crate::message::message::KafkaMessage;
use rdkafka::admin::{AdminClient, AdminOptions, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::StreamConsumer;
use rdkafka::producer::FutureProducer;
use std::time::Duration;
use uuid::Uuid;

pub struct Kafka {
    pub config: KafkaConfig,
    pub admin: AdminClient<DefaultClientContext>,
    pub producer: FutureProducer,
    pub consumer: StreamConsumer,
}

impl Kafka {
    pub async fn new(config: &KafkaConfig) -> DMSRResult<Self> {
        let mut admin_config = ClientConfig::new();
        admin_config.set("bootstrap.servers", &config.bootstrap_servers);
        let admin: AdminClient<DefaultClientContext> = admin_config.create()?;

        let mut producer_config = ClientConfig::new();
        producer_config.set("bootstrap.servers", &config.bootstrap_servers);
        let producer: FutureProducer = producer_config.create()?;

        let mut consumer_config = ClientConfig::new();
        consumer_config.set("group.id", Uuid::new_v4().to_string());
        consumer_config.set("bootstrap.servers", &config.bootstrap_servers);
        consumer_config.set("auto.offset.reset", "smallest");
        let consumer: StreamConsumer = consumer_config.create()?;

        Ok(Kafka {
            config: config.clone(),
            admin,
            producer,
            consumer,
        })
    }

    pub async fn create_config_topic(&self) -> DMSRResult<()> {
        let topic_name = &self.config.config_topic;
        let topic = rdkafka::admin::NewTopic::new(topic_name, 1, TopicReplication::Fixed(1))
            .set("cleanup.policy", "compact");
        let topics = [topic];
        let options = AdminOptions::new();
        let result = self.admin.create_topics(&topics, &options).await?;
        println!("Result: {:?}", result);
        Ok(())
    }

    pub async fn ingest<Source>(
        &self,
        topic: String,
        message: &KafkaMessage<Source>,
        key: Option<String>,
    ) -> DMSRResult<()> where Source: serde::Serialize + serde::de::DeserializeOwned {
        let message = serde_json::to_string(message)?;

        let mut record =
            rdkafka::producer::FutureRecord::to(topic.as_str()).payload(message.as_str());

        let key = key.unwrap_or_default();
        if !key.is_empty() {
            record = record.key(key.as_str());
        }

        let status = self.producer.send(record, Duration::from_secs(0)).await;
        return match status {
            Ok(delivery_status) => {
                println!("Delivery status: {:?}", delivery_status);
                Ok(())
            }
            Err((err, message)) => {
                println!("Send failed: {:?}: {:?}", err, message);
                Err(err.into())
            }
        };
    }
}
