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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_mysql_offset_output() {
        let offset_output = MySQLOffsetOutput::new(
            "test_connector".to_string(),
            "test_db".to_string(),
            1234,
            "test_file".to_string(),
            5678,
        );
        assert_eq!(offset_output.connector_name, "test_connector");
        assert_eq!(offset_output.db, "test_db");
        assert_eq!(offset_output.server_id, 1234);
        assert_eq!(offset_output.file, "test_file");
        assert_eq!(offset_output.pos, 5678);
    }

    #[test]
    fn test_clone_mysql_offset_output() {
        let offset_output = MySQLOffsetOutput::new(
            "test_connector".to_string(),
            "test_db".to_string(),
            1234,
            "test_file".to_string(),
            5678,
        );
        let cloned_offset_output = offset_output.clone();
        assert_eq!(offset_output, cloned_offset_output);
    }

    #[test]
    fn test_file_and_pos_methods() {
        let offset_output = MySQLOffsetOutput::new(
            "test_connector".to_string(),
            "test_db".to_string(),
            1234,
            "test_file".to_string(),
            5678,
        );
        let file = offset_output.file();
        let pos = offset_output.pos();
        assert_eq!(file, "test_file");
        assert_eq!(pos, 5678);
    }

    #[test]
    fn test_to_kafka_message_default() {
        let offset_output = MySQLOffsetOutput::new(
            "test_connector".to_string(),
            "test_db".to_string(),
            1234,
            "test_file".to_string(),
            5678,
        );

        let kafka_config = KafkaConfig::new(
            "localhost:9092".to_string(),
            "test_config".to_string(),
            "test_offset".to_string(),
            "test_status".to_string(),
        );

        let kafka_message = offset_output
            .to_kafka_message_default(&kafka_config)
            .unwrap();
        let key = kafka_message.key;
        let value = kafka_message.value;
        let topic = kafka_message.topic;

        let expected_key = json!({
            "connector_name": "test_connector"
        })
        .to_string();
        let expected_value = serde_json::to_string(&offset_output).unwrap();
        let expected_topic = "test_offset";

        assert_eq!(key, Some(expected_key));
        assert_eq!(value, expected_value);
        assert_eq!(topic, expected_topic);
    }
}
