use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct KafkaConfig {
    pub bootstrap_servers: String,
    pub config_topic: String,
    pub offset_topic: String,
    pub status_topic: String,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        KafkaConfig::new(
            "localhost:9092".to_string(),
            "dmsr_config".to_string(),
            "dmsr_offset".to_string(),
            "dmsr_status".to_string(),
        )
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default() {
        let config = KafkaConfig::default();
        assert_eq!(
            config,
            KafkaConfig::new(
                "localhost:9092".to_string(),
                "dmsr_config".to_string(),
                "dmsr_offset".to_string(),
                "dmsr_status".to_string(),
            )
        );
    }
}
