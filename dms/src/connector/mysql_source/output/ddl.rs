use crate::connector::mysql_source::metadata::source::MySQLSourceMetadata;
use crate::error::DMSRResult;
use crate::kafka::config::KafkaConfig;
use crate::kafka::message::KafkaMessage;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct MySQLDDLOutput {
    pub ddl: String,
    pub ts_ms: u64,
    pub metadata: MySQLSourceMetadata,
}

impl MySQLDDLOutput {
    pub fn new(ddl: String, ts_ms: u64, metadata: MySQLSourceMetadata) -> Self {
        MySQLDDLOutput {
            ddl,
            ts_ms,
            metadata,
        }
    }

    pub fn to_kafka_message_default(&self, kafka_config: &KafkaConfig) -> DMSRResult<KafkaMessage> {
        let mut message = KafkaMessage::new();

        let mut key = json!({});
        key["schema"] = self.metadata.schema().to_string().into();
        key["table"] = self.metadata.table().to_string().into();
        let key = serde_json::to_string(&key)?;

        message.set_key(key);

        let value = serde_json::to_string(&self)?;
        message.set_value(value);

        let topic = self.metadata.connector_name();
        message.set_topic(topic.to_string());

        Ok(message)
    }
}
