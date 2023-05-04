use crate::error::error::DMSRResult;
use crate::kafka::message::KafkaMessage;
use crate::kafka::metadata::ConnectorMetadata;
use crate::kafka::payload::base::{Operation, PayloadEncoding};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct RowDataPayload<M>
where
    M: ConnectorMetadata,
{
    pub before: Option<serde_json::Value>,
    pub after: Option<serde_json::Value>,
    pub op: Operation,
    pub ts_ms: u64,
    pub metadata: M,
}

impl<M> RowDataPayload<M>
where
    M: ConnectorMetadata,
{
    pub fn new(
        before: Option<serde_json::Value>,
        after: Option<serde_json::Value>,
        op: Operation,
        ts_ms: u64,
        metadata: M,
    ) -> Self {
        RowDataPayload {
            before,
            after,
            op,
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
