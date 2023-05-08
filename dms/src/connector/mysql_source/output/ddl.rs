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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_mysql_ddl_output() {
        let metadata = MySQLSourceMetadata::new(
            "test_connector",
            "test_db",
            "test_schema",
            "test_table",
            1234,
            "test_file",
            5678,
        );
        let ddl_output = MySQLDDLOutput::new("test_ddl".to_string(), 9876, metadata.clone());
        assert_eq!(ddl_output.ddl, "test_ddl");
        assert_eq!(ddl_output.ts_ms, 9876);
        assert_eq!(ddl_output.metadata, metadata);
    }

    #[test]
    fn test_clone_mysql_ddl_output() {
        let metadata = MySQLSourceMetadata::new(
            "test_connector",
            "test_db",
            "test_schema",
            "test_table",
            1234,
            "test_file",
            5678,
        );
        let ddl_output = MySQLDDLOutput::new("test_ddl".to_string(), 9876, metadata.clone());
        let cloned_ddl_output = ddl_output.clone();
        assert_eq!(ddl_output, cloned_ddl_output);
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
        let ddl_output = MySQLDDLOutput::new("test_ddl".to_string(), 9876, metadata.clone());

        let kafka_config = KafkaConfig::new(
            "test_bootstrap_servers".to_string(),
            "test_config".to_string(),
            "test_offset".to_string(),
            "test_status".to_string(),
        );

        let kafka_message = ddl_output.to_kafka_message_default(&kafka_config).unwrap();
        let key = kafka_message.key;
        let value = kafka_message.value;
        let topic = kafka_message.topic;

        let expected_key = json!({
            "schema": "test_schema",
            "table": "test_table"
        })
        .to_string();
        let expected_value = serde_json::to_string(&ddl_output).unwrap();
        let expected_topic = "test_connector";

        assert_eq!(key, Some(expected_key));
        assert_eq!(value, expected_value);
        assert_eq!(topic, expected_topic);
    }
}
