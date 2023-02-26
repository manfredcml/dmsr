use crate::queue::kafka_config::KafkaConfig;
use crate::queue::queue::Queue;
use crate::queue::queue_config::QueueConfig;
use crate::queue::queue_kind::QueueKind;
use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureProducer;
use std::time::Duration;
use crate::events::standardized_event::Event;

pub struct Kafka {
    config: KafkaConfig,
    producer: Option<FutureProducer>,
}

#[async_trait]
impl Queue for Kafka {
    fn new(config: &QueueConfig) -> anyhow::Result<Box<Self>> {
        if config.kind != QueueKind::Kafka {
            return Err(anyhow::anyhow!("Invalid streamer type"));
        }

        let kafka_config = match config.kafka_config.clone() {
            Some(kafka_config) => kafka_config,
            None => return Err(anyhow::anyhow!("Missing Kafka config")),
        };

        Ok(Box::new(Kafka {
            config: kafka_config,
            producer: None,
        }))
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        let mut producer_config = ClientConfig::new();
        producer_config.set("bootstrap.servers", &self.config.bootstrap_servers);
        let producer: FutureProducer = producer_config.create()?;
        self.producer = Some(producer);
        Ok(())
    }

    async fn ingest(&mut self, event: Event) -> anyhow::Result<()> {
        let producer = match &self.producer {
            Some(producer) => producer,
            None => return Err(anyhow::anyhow!("Producer not initialized")),
        };

        let message = "Hello, Kafka!";
        let key = "my_key";
        let topic = "test_topic";

        let record = rdkafka::producer::FutureRecord::to(topic)
            .payload(message)
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
