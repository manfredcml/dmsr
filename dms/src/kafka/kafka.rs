use crate::error::error::DMSRResult;
use crate::kafka::config::KafkaConfig;
use crate::kafka::message::KafkaMessage;
use log::debug;
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

    pub async fn create_topic(&self, topic_name: &str, clean_up_policy: &str) -> DMSRResult<()> {
        let topic = rdkafka::admin::NewTopic::new(topic_name, 1, TopicReplication::Fixed(1))
            .set("cleanup.policy", clean_up_policy);
        let topics = [topic];
        let options = AdminOptions::new();
        let result = self.admin.create_topics(&topics, &options).await?;
        println!("Result: {:?}", result);
        Ok(())
    }

    pub async fn produce(&self, message: KafkaMessage) -> DMSRResult<()> {
        let topic = &message.topic;
        let value = &message.value;
        let key = &message.key.unwrap_or_default();
        let mut record = rdkafka::producer::FutureRecord::to(topic).payload(value);

        if !key.is_empty() {
            record = record.key(key);
        }

        let status = self.producer.send(record, Duration::from_secs(0)).await;
        match status {
            Ok(delivery_status) => {
                debug!("Delivery status: {:?}\n", delivery_status);
                Ok(())
            }
            Err((err, message)) => {
                debug!("Send failed: {:?}: {:?}\n", err, message);
                Err(err.into())
            }
        }
    }
}
