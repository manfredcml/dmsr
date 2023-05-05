use crate::connector::mysql_source::source_metadata::MySQLSourceMetadata;
use crate::error::error::DMSRResult;
use crate::kafka::config::KafkaConfig;
use crate::kafka::message::KafkaMessage;
use crate::kafka::metadata::ConnectorMetadata;
use crate::kafka::payload::base::{Operation, PayloadEncoding};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub enum RowDataPayload {
    MySQL(MySQLRowDataPayload),
}

impl RowDataPayload {
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
            RowDataPayload::MySQL(payload) => {
                let mut message = KafkaMessage::new();

                // if let Some(key) = &self.primary_keys {
                //     let key = serde_json::to_string(key)?;
                //     message.set_key(key);
                // }

                let value = serde_json::to_string(&payload)?;
                message.set_value(value);

                let topic = payload.metadata.kafka_topic();
                message.set_topic(topic);

                Ok(message)
            }
        }
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct MySQLRowDataPayload {
    before: Option<serde_json::Value>,
    after: Option<serde_json::Value>,
    op: Operation,
    ts_ms: u64,
    metadata: MySQLSourceMetadata,
}

impl MySQLRowDataPayload {
    pub fn new(
        before: Option<serde_json::Value>,
        after: Option<serde_json::Value>,
        op: Operation,
        ts_ms: u64,
        metadata: MySQLSourceMetadata,
    ) -> Self {
        MySQLRowDataPayload {
            before,
            after,
            op,
            ts_ms,
            metadata,
        }
    }
}
