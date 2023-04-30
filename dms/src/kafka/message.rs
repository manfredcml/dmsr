use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct KafkaMessage {
    pub topic: String,
    pub key: Option<String>,
    pub value: String,
}

impl KafkaMessage {
    pub fn new() -> Self {
        KafkaMessage {
            topic: String::default(),
            key: None,
            value: String::default(),
        }
    }

    pub fn set_topic(&mut self, topic: String) {
        self.topic = topic;
    }

    pub fn set_key(&mut self, key: String) {
        self.key = Some(key);
    }

    pub fn set_value(&mut self, value: String) {
        self.value = value;
    }
}
