use crate::connector::mysql_source::metadata::source::MySQLSourceMetadata;
use crate::connector::row_data_operation::Operation;
use crate::error::DMSRResult;
use crate::kafka::config::KafkaConfig;
use crate::kafka::message::KafkaMessage;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct MySQLRowDataOutput {
    before: Option<serde_json::Value>,
    after: Option<serde_json::Value>,
    op: Operation,
    ts_ms: u64,
    metadata: MySQLSourceMetadata,
}

impl MySQLRowDataOutput {
    pub fn new(
        before: Option<serde_json::Value>,
        after: Option<serde_json::Value>,
        op: Operation,
        ts_ms: u64,
        metadata: MySQLSourceMetadata,
    ) -> Self {
        MySQLRowDataOutput {
            before,
            after,
            op,
            ts_ms,
            metadata,
        }
    }

    pub fn to_kafka_message_default(&self, kafka_config: &KafkaConfig) -> DMSRResult<KafkaMessage> {
        let mut message = KafkaMessage::new();

        // if let Some(key) = &self.primary_keys {
        //     let key = serde_json::to_string(key)?;
        //     message.set_key(key);
        // }

        let value = serde_json::to_string(&self)?;
        message.set_value(value);

        let topic = self.metadata.kafka_topic();
        message.set_topic(topic);

        Ok(message)
    }
}
