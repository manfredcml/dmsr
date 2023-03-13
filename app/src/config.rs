use dms::kafka::config::KafkaConfig;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct AppConfig {
    pub kafka: KafkaConfig,
    pub port: u16,
}
