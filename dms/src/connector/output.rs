use crate::connector::mysql_source::output::ddl::MySQLDDLOutput;
use crate::connector::mysql_source::output::offset::MySQLOffsetOutput;
use crate::connector::mysql_source::output::row_data::MySQLRowDataOutput;
use crate::error::DMSRResult;
use crate::kafka::config::KafkaConfig;
use crate::kafka::message::KafkaMessage;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub enum ConnectorOutput {
    MySQLRowData(MySQLRowDataOutput),
    MySQLDDL(MySQLDDLOutput),
    MySQLOffset(MySQLOffsetOutput),
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub enum OutputEncoding {
    Default,
}

impl ConnectorOutput {
    pub fn to_kafka_message(
        &self,
        config: &KafkaConfig,
        encoding: OutputEncoding,
    ) -> DMSRResult<KafkaMessage> {
        match self {
            ConnectorOutput::MySQLRowData(output) => match encoding {
                OutputEncoding::Default => output.to_kafka_message_default(config),
            },
            ConnectorOutput::MySQLDDL(output) => match encoding {
                OutputEncoding::Default => output.to_kafka_message_default(config),
            },
            ConnectorOutput::MySQLOffset(output) => match encoding {
                OutputEncoding::Default => output.to_kafka_message_default(config),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::mysql_source::metadata::source::MySQLSourceMetadata;
    use crate::connector::row_data_operation::Operation;
    use serde_json::json;

    #[test]
    fn test_to_kafka_message_mysql_row_data_default_encoding() {
        let before = json!({
            "id": 1,
            "name": "before",
        });

        let after = json!({
            "id": 2,
            "name": "after",
        });

        let output = ConnectorOutput::MySQLRowData(MySQLRowDataOutput::new(
            Some(before),
            Some(after),
            Operation::Update,
            12345,
            MySQLSourceMetadata::new(
                "test_connector",
                "test_db",
                "test_schema",
                "test_table",
                999,
                "test_file",
                777,
            ),
        ));

        let config = KafkaConfig::new(
            "localhost:9092".to_string(),
            "test_config".to_string(),
            "test_offset".to_string(),
            "test_status".to_string(),
        );

        let msg = output
            .to_kafka_message(&config, OutputEncoding::Default)
            .unwrap();

        assert_eq!(
            msg.topic,
            "test_connector.test_schema.test_table".to_string()
        );
        assert_eq!(msg.key, None);
        assert_eq!(
            msg.value,
            r#"{"before":{"id":1,"name":"before"},"after":{"id":2,"name":"after"},"op":"u","ts_ms":12345,"metadata":{"connector_type":"mysql_source","connector_name":"test_connector","db":"test_db","schema":"test_schema","table":"test_table","server_id":999,"file":"test_file","pos":777}}"#
        );
    }

    #[test]
    fn test_to_kafka_message_mysql_ddl_data_default_encoding() {
        let output = MySQLDDLOutput::new(
            "CREATE TABLE XXX".to_string(),
            12345,
            MySQLSourceMetadata::new(
                "test_connector",
                "test_db",
                "test_schema",
                "test_table",
                999,
                "test_file",
                777,
            ),
        );

        let output = ConnectorOutput::MySQLDDL(output);
        let config = KafkaConfig::new(
            "localhost:9092".to_string(),
            "test_config".to_string(),
            "test_offset".to_string(),
            "test_status".to_string(),
        );
        let msg = output
            .to_kafka_message(&config, OutputEncoding::Default)
            .unwrap();

        assert_eq!(msg.topic, "test_connector".to_string());
        assert_eq!(
            msg.key,
            Some(r#"{"schema":"test_schema","table":"test_table"}"#.to_string())
        );
        assert_eq!(
            msg.value,
            r#"{"ddl":"CREATE TABLE XXX","ts_ms":12345,"metadata":{"connector_type":"mysql_source","connector_name":"test_connector","db":"test_db","schema":"test_schema","table":"test_table","server_id":999,"file":"test_file","pos":777}}"#
        );
    }

    #[test]
    fn test_to_kafka_message_mysql_offset_data_default_encoding() {
        let output = MySQLOffsetOutput::new(
            "test-connector".to_string(),
            "test-db".to_string(),
            1,
            "test-file".to_string(),
            999,
        );

        let output = ConnectorOutput::MySQLOffset(output);

        let config = KafkaConfig::new(
            "localhost:9092".to_string(),
            "test_config".to_string(),
            "test_offset".to_string(),
            "test_status".to_string(),
        );
        let msg = output
            .to_kafka_message(&config, OutputEncoding::Default)
            .unwrap();

        assert_eq!(msg.topic, "test_offset".to_string());
        assert_eq!(msg.key, Some(r#"{"connector_name":"test-connector"}"#.into()));
        assert_eq!(
            msg.value,
            r#"{"connector_name":"test-connector","db":"test-db","server_id":1,"file":"test-file","pos":999}"#
        );
    }
}
