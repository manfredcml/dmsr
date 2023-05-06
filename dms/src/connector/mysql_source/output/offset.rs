use crate::error::DMSRResult;
use crate::kafka::config::KafkaConfig;
use crate::kafka::message::KafkaMessage;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct MySQLOffsetOutput {
    connector_name: String,
    db: String,
    server_id: u32,
    file: String,
    pos: u64,
}

impl MySQLOffsetOutput {
    pub fn new(connector_name: String, db: String, server_id: u32, file: String, pos: u64) -> Self {
        MySQLOffsetOutput {
            connector_name,
            db,
            server_id,
            file,
            pos,
        }
    }

    pub fn file(&self) -> &str {
        &self.file
    }

    pub fn pos(&self) -> u64 {
        self.pos
    }

    pub fn to_kafka_message_default(&self, kafka_config: &KafkaConfig) -> DMSRResult<KafkaMessage> {
        let mut message = KafkaMessage::new();

        let mut key = json!({});
        key["connector_name"] = self.connector_name.clone().into();
        let key = serde_json::to_string(&key)?;

        message.set_key(key);

        let value = serde_json::to_string(&self)?;
        message.set_value(value);

        let topic = kafka_config.offset_topic.clone();
        message.set_topic(topic);

        Ok(message)
    }
}
