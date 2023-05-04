use crate::error::error::DMSRResult;
use crate::kafka::config::KafkaConfig;
use crate::kafka::message::KafkaMessage;
use crate::kafka::payload::base::PayloadEncoding;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub enum OffsetPayload {
    MySQL(MySQLOffsetPayload),
}

impl OffsetPayload {
    pub fn to_kafka_message(
        &self,
        kafka_config: &KafkaConfig,
        encoding: PayloadEncoding,
    ) -> DMSRResult<KafkaMessage> {
        match encoding {
            PayloadEncoding::Default => self.to_kafka_message_default(kafka_config),
        }
    }

    fn to_kafka_message_default(&self, kafka_config: &KafkaConfig) -> DMSRResult<KafkaMessage> {
        match self {
            OffsetPayload::MySQL(payload) => {
                let mut message = KafkaMessage::new();

                let mut key = json!({});
                key["connector_name"] = payload.connector_name.clone().into();
                let key = serde_json::to_string(&key)?;

                message.set_key(key);

                let value = serde_json::to_string(&payload)?;
                message.set_value(value);

                let topic = kafka_config.offset_topic.clone();
                message.set_topic(topic);

                Ok(message)
            }
        }
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct MySQLOffsetPayload {
    connector_name: String,
    db: String,
    server_id: u32,
    file: String,
    pos: u64,
}

impl MySQLOffsetPayload {
    pub fn new(connector_name: &str, db: &str, server_id: u32, file: &str, pos: u64) -> Self {
        MySQLOffsetPayload {
            connector_name: connector_name.to_string(),
            db: db.to_string(),
            server_id,
            file: file.to_string(),
            pos,
        }
    }
}
