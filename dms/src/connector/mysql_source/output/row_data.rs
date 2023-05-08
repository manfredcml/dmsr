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

mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_new_mysql_row_data_output() {
        let metadata = MySQLSourceMetadata::new(
            "test_connector",
            "test_db",
            "test_schema",
            "test_table",
            1234,
            "test_file",
            5678,
        );
        let row_data_output = MySQLRowDataOutput::new(
            Some(json!({"id": 1})),
            Some(json!({"id": 2})),
            Operation::Update,
            9876,
            metadata.clone(),
        );
        assert_eq!(row_data_output.before, Some(json!({"id": 1})));
        assert_eq!(row_data_output.after, Some(json!({"id": 2})));
        assert_eq!(row_data_output.op, Operation::Update);
        assert_eq!(row_data_output.ts_ms, 9876);
        assert_eq!(row_data_output.metadata, metadata);
    }

    #[test]
    fn test_clone_mysql_row_data_output() {
        let metadata = MySQLSourceMetadata::new(
            "test_connector",
            "test_db",
            "test_schema",
            "test_table",
            1234,
            "test_file",
            5678,
        );
        let row_data_output = MySQLRowDataOutput::new(
            Some(json!({"id": 1})),
            Some(json!({"id": 2})),
            Operation::Update,
            9876,
            metadata.clone(),
        );
        let cloned_row_data_output = row_data_output.clone();
        assert_eq!(row_data_output, cloned_row_data_output);
    }

    #[test]
    fn test_to_kafka_message_default() {
        let metadata = MySQLSourceMetadata::new(
            "test_connector",
            "test_db",
            "test_schema",
            "test_table",
            1234,
            "test_file",
            5678,
        );
        let row_data_output = MySQLRowDataOutput::new(
            Some(json!({"id": 1})),
            Some(json!({"id": 2})),
            Operation::Update,
            9876,
            metadata.clone(),
        );

        let kafka_config = KafkaConfig::new(
            "localhost:9092".to_string(),
            "test_config".to_string(),
            "test_offset".to_string(),
            "test_status".to_string(),
        );

        let kafka_message = row_data_output
            .to_kafka_message_default(&kafka_config)
            .unwrap();
        let key = kafka_message.key;
        let value = kafka_message.value;
        let topic = kafka_message.topic;

        let expected_value = serde_json::to_string(&row_data_output).unwrap();
        let expected_topic = "test_connector.test_schema.test_table";

        assert_eq!(key, None);
        assert_eq!(value, expected_value);
        assert_eq!(topic, expected_topic);
    }
}
