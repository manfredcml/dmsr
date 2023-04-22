use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct KafkaMessage {
    pub topic: String,
    pub key: Option<String>,
    pub value: String,
}

impl KafkaMessage {
    pub fn new(topic: String, key: Option<String>, value: String) -> Self {
        KafkaMessage { topic, key, value }
    }
}
