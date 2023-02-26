use crate::queue::kafka::Kafka;
use crate::queue::kafka_config::KafkaConfig;
use crate::queue::queue::Queue;
use crate::queue::queue_kind::QueueKind;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct QueueConfig {
    #[serde(deserialize_with = "deserialize_streamer_type")]
    pub kind: QueueKind,
    pub name: String,
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

fn deserialize_streamer_type<'de, D>(deserializer: D) -> Result<QueueKind, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    match s.to_lowercase().as_str() {
        "kafka" => Ok(QueueKind::Kafka),
        _ => Err(serde::de::Error::custom(format!(
            "Unknown source type: {}",
            s
        ))),
    }
}
