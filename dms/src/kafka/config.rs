use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct KafkaConfig {
    pub bootstrap_servers: String,
    pub config_topic: String,
    pub offset_topic: String,
    pub status_topic: String,
}

impl KafkaConfig {
    pub fn new(
        bootstrap_servers: String,
        config_topic: String,
        offset_topic: String,
        status_topic: String,
    ) -> Self {
        KafkaConfig {
            bootstrap_servers,
            config_topic,
            offset_topic,
            status_topic,
        }
    }
}
