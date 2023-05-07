use crate::error::DMSRResult;
use crate::kafka::config::KafkaConfig;
use crate::kafka::message::KafkaMessage;
use log::debug;
use rdkafka::admin::{AdminClient, AdminOptions, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, StreamConsumer};
use rdkafka::producer::FutureProducer;
use rdkafka::Message;
use std::time::Duration;
use tokio::time::Instant;
use uuid::Uuid;

#[derive(Debug)]
pub struct RawKafkaMessageKeyValue {
    pub key: String,
    pub value: String,
}

impl RawKafkaMessageKeyValue {
    pub fn new(key: String, value: String) -> Self {
        RawKafkaMessageKeyValue { key, value }
    }
}

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
                debug!("Send success - {:?} {:?}", topic, delivery_status);
                Ok(())
            }
            Err((err, message)) => {
                debug!("Send failed: {:?}: {:?}", err, message);
                Err(err.into())
            }
        }
    }

    pub fn get_base_consumer(&self, topic: &str) -> DMSRResult<BaseConsumer> {
        let mut config = ClientConfig::new();
        config.set("group.id", Uuid::new_v4().to_string());
        config.set("bootstrap.servers", &self.config.bootstrap_servers);
        config.set("auto.offset.reset", "smallest");
        let consumer: BaseConsumer = config.create()?;
        consumer.subscribe(&[topic])?;
        Ok(consumer)
    }

    pub fn get_stream_consumer(&self, topic: &str) -> DMSRResult<StreamConsumer> {
        let mut config = ClientConfig::new();
        config.set("group.id", Uuid::new_v4().to_string());
        config.set("bootstrap.servers", &self.config.bootstrap_servers);
        config.set("auto.offset.reset", "smallest");
        let consumer: StreamConsumer = config.create()?;
        consumer.subscribe(&[topic])?;
        Ok(consumer)
    }

    pub async fn poll_with_timeout(
        &self,
        topic: &str,
        timeout: u64,
    ) -> DMSRResult<Vec<RawKafkaMessageKeyValue>> {
        let mut config = ClientConfig::new();
        config.set("group.id", Uuid::new_v4().to_string());
        config.set("bootstrap.servers", &self.config.bootstrap_servers);
        config.set("auto.offset.reset", "earliest");
        let consumer: BaseConsumer = config.create()?;

        consumer.subscribe(&[topic])?;

        let mut messages: Vec<RawKafkaMessageKeyValue> = vec![];
        let mut last_received = Instant::now();
        loop {
            let message = consumer.poll(Duration::from_secs(timeout));
            match message {
                Some(Ok(message)) => {
                    last_received = Instant::now();
                    let key = message.key().unwrap_or_default();
                    let key = std::str::from_utf8(key)?.to_string();
                    let value = message.payload().unwrap_or_default();
                    let value = std::str::from_utf8(value)?.to_string();
                    let raw_message = RawKafkaMessageKeyValue::new(key, value);
                    messages.push(raw_message);
                }
                Some(Err(err)) => {
                    return Err(err.into());
                }
                None => {
                    if last_received.elapsed().as_secs() >= timeout {
                        break;
                    }
                }
            }
        }

        Ok(messages)
    }
}
