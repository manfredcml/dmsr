use dms::kafka::kafka_config::KafkaConfig;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct AppConfig {
    pub kafka: KafkaConfig,
}
