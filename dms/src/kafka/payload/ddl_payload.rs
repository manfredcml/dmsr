use crate::error::error::DMSRResult;
use crate::kafka::message::KafkaMessage;
use crate::kafka::metadata::ConnectorMetadata;
use crate::kafka::payload::base::PayloadEncoding;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct DDLPayload<M>
where
    M: ConnectorMetadata,
{
    pub ddl: String,
    pub ts_ms: u64,
    pub metadata: M,
}

impl<M> DDLPayload<M>
where
    M: ConnectorMetadata,
{
    pub fn new(ddl: String, ts_ms: u64, metadata: M) -> Self {
        DDLPayload {
            ddl,
            ts_ms,
            metadata,
        }
    }

    pub fn into_kafka_message(self, encoding: PayloadEncoding) -> DMSRResult<KafkaMessage> {
        match encoding {
            PayloadEncoding::Default => self.into_kafka_message_default(),
        }
    }

    fn into_kafka_message_default(self) -> DMSRResult<KafkaMessage> {
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
