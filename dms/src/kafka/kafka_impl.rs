use crate::error::generic::{DMSRError, DMSRResult};
use crate::error::missing_value::MissingValueError;
use crate::event::event::ChangeEvent;
use crate::kafka::kafka_config::KafkaConfig;
use rdkafka::admin::{AdminClient, AdminOptions, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureProducer;
use std::error::Error;
use std::time::Duration;

pub struct Kafka {
    config: KafkaConfig,
    admin: Option<AdminClient<DefaultClientContext>>,
    producer: Option<FutureProducer>,
}

impl Kafka {
    fn new(config: &KafkaConfig) -> DMSRResult<Box<Self>> {
        Ok(Box::new(Kafka {
            config: config.clone(),
            admin: None,
            producer: None,
        }))
    }

    async fn connect(&mut self) -> DMSRResult<()> {
        let mut producer_config = ClientConfig::new();
        producer_config.set("bootstrap.servers", &self.config.bootstrap_servers);
        let producer: FutureProducer = producer_config.create()?;
        self.producer = Some(producer);

        let mut admin_config = ClientConfig::new();
        admin_config.set("bootstrap.servers", &self.config.bootstrap_servers);
        let admin = admin_config.create()?;
        self.admin = Some(admin);

        Ok(())
    }

    async fn create_default_topics(&mut self) -> DMSRResult<()> {
        let admin = match &self.admin {
            Some(admin) => admin,
            None => {
                let err = MissingValueError {
                    field_name: "admin",
                };
                return Err(DMSRError::from(err));
            }
        };

        let topic_name = "test_topic";
        let topic = rdkafka::admin::NewTopic::new(topic_name, 1, TopicReplication::Fixed(1));
        let topic = topic.set("cleanup.policy", "compact");
        let topics = [topic];
        let options = AdminOptions::new();

        let result = admin.create_topics(&topics, &options).await?;
        println!("Result: {:?}", result);

        Ok(())
    }

    async fn ingest(&mut self, event: ChangeEvent) -> anyhow::Result<()> {
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
