use crate::streamers::kafka::Kafka;
use crate::streamers::kafka_config::KafkaConfig;
use crate::streamers::streamer::Streamer;
use crate::streamers::streamer_type::StreamerKind;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct StreamerConfig {
    #[serde(deserialize_with = "deserialize_streamer_type")]
    pub kind: StreamerKind,
    pub name: String,
    pub kafka_config: Option<KafkaConfig>,
}

impl StreamerConfig {
    pub fn get_streamer(&self) -> anyhow::Result<Box<dyn Streamer + Send>> {
        match self.kind {
            StreamerKind::Kafka => {
                let kafka = Kafka::new(&self)?;
                Ok(kafka)
            }
        }
    }
}

fn deserialize_streamer_type<'de, D>(deserializer: D) -> Result<StreamerKind, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    match s.to_lowercase().as_str() {
        "kafka" => Ok(StreamerKind::Kafka),
        _ => Err(serde::de::Error::custom(format!(
            "Unknown source type: {}",
            s
        ))),
    }
}
