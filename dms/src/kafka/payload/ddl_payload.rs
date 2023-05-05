use crate::connector::mysql_source::source_metadata::MySQLSourceMetadata;
use crate::error::error::DMSRResult;
use crate::kafka::config::KafkaConfig;
use crate::kafka::message::KafkaMessage;
use crate::kafka::metadata::ConnectorMetadata;
use crate::kafka::payload::base::PayloadEncoding;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub enum DDLPayload {
    MySQL(MySQLDDLPayload),
}

impl DDLPayload {
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
            DDLPayload::MySQL(payload) => {
                let mut message = KafkaMessage::new();

                let mut key = json!({});
                key["schema"] = payload.metadata.schema().to_string().into();
                key["table"] = payload.metadata.table().to_string().into();
                let key = serde_json::to_string(&key)?;

                message.set_key(key);

                let value = serde_json::to_string(&payload)?;
                message.set_value(value);

                let topic = payload.metadata.connector_name();
                message.set_topic(topic.to_string());

                Ok(message)
            }
        }
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct MySQLDDLPayload {
    pub ddl: String,
    pub ts_ms: u64,
    pub metadata: MySQLSourceMetadata,
}

impl MySQLDDLPayload {
    pub fn new(ddl: &str, ts_ms: u64, metadata: MySQLSourceMetadata) -> Self {
        MySQLDDLPayload {
            ddl: ddl.to_string(),
            ts_ms,
            metadata,
        }
    }
}
