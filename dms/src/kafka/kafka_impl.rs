use crate::error::generic::{DMSRError, DMSRResult};
use crate::error::missing_value::MissingValueError;
use crate::event::event::ChangeEvent;
use crate::kafka::kafka_config::KafkaConfig;
use rdkafka::admin::{AdminClient, AdminOptions, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::StreamConsumer;
use rdkafka::producer::FutureProducer;
use std::time::Duration;
use uuid::Uuid;

pub struct Kafka {
    pub config: KafkaConfig,
    pub admin: Option<AdminClient<DefaultClientContext>>,
    pub producer: Option<FutureProducer>,
    pub consumer: Option<StreamConsumer>,
}

impl Kafka {
    pub fn new(config: &KafkaConfig) -> DMSRResult<Self> {
        Ok(Kafka {
            config: config.clone(),
            admin: None,
            producer: None,
            consumer: None,
        })
    }

    pub async fn connect(&mut self) -> DMSRResult<()> {
        let mut producer_config = ClientConfig::new();
        producer_config.set("bootstrap.servers", &self.config.bootstrap_servers);
        let producer: FutureProducer = producer_config.create()?;
        self.producer = Some(producer);

        let mut admin_config = ClientConfig::new();
        admin_config.set("bootstrap.servers", &self.config.bootstrap_servers);
        let admin = admin_config.create()?;
        self.admin = Some(admin);

        let mut consumer_config = ClientConfig::new();
        consumer_config.set("group.id", Uuid::new_v4().to_string());
        consumer_config.set("bootstrap.servers", &self.config.bootstrap_servers);
        consumer_config.set("auto.offset.reset", "smallest");
        let consumer = consumer_config.create()?;
        self.consumer = Some(consumer);

        Ok(())
    }

    pub async fn create_config_topic(&mut self) -> DMSRResult<()> {
        let admin = match &self.admin {
            Some(admin) => admin,
            None => {
                let err = MissingValueError {
                    field_name: "admin",
                };
                return Err(DMSRError::from(err));
            }
        };

        let topic_name = &self.config.config_topic;
        let topic = rdkafka::admin::NewTopic::new(topic_name, 1, TopicReplication::Fixed(1)).set("cleanup.policy", "compact");
        let topics = [topic];
        let options = AdminOptions::new();
        let result = admin.create_topics(&topics, &options).await?;
        println!("Result: {:?}", result);

        Ok(())
    }

    pub async fn ingest(&mut self, event: ChangeEvent) -> anyhow::Result<()> {
        let producer = match &self.producer {
            Some(producer) => producer,
            None => return Err(anyhow::anyhow!("Producer not initialized")),
        };

        let key = "my_key";
        let topic = "test_topic";
        let message = serde_json::to_string(&event)?;

        println!("Message: {}", message);

        let record = rdkafka::producer::FutureRecord::to(topic)
            .payload(message.as_str())
            .key(key);

        let status = producer.send(record, Duration::from_secs(0)).await;
        return match status {
            Ok(delivery_status) => {
                println!("Delivery status: {:?}", delivery_status);
                Ok(())
            }
            Err((err, message)) => {
                println!("Send failed: {:?}: {:?}", err, message);
                Err(anyhow::anyhow!("Send failed: {:?}: {:?}", err, message))
            }
        };
    }
}
