use crate::queue::kafka::Kafka;
use crate::queue::kafka_config::KafkaConfig;
use crate::queue::queue::Queue;
use crate::queue::kind::QueueKind;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct QueueConfig {
    pub kind: QueueKind,
    pub name: String,
    #[serde(rename = "kafka")]
    pub kafka_config: Option<KafkaConfig>,
}

impl QueueConfig {
    pub fn get_streamer(&self) -> anyhow::Result<Box<dyn Queue + Send>> {
        match self.kind {
            QueueKind::Kafka => {
                let kafka = Kafka::new(&self)?;
                Ok(kafka)
            }
        }
    }
}
