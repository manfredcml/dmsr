use crate::connector::kind::ConnectorKind;
use crate::kafka::message::{KafkaJSONField, KafkaSchemaDataType};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct MySQLSourceMetadata {
    pub connector_type: ConnectorKind,
    pub connector_name: String,
    pub db: String,
    pub table: String,
    pub server_id: u32,
    pub file: String,
    pub pos: u64,
}

impl MySQLSourceMetadata {
    pub fn new(
        connector_name: String,
        db: String,
        table: String,
        server_id: u32,
        file: String,
        pos: u64,
    ) -> Self {
        MySQLSourceMetadata {
            connector_type: ConnectorKind::MySQLSource,
            connector_name,
            db,
            table,
            server_id,
            file,
            pos,
        }
    }

    pub fn get_schema() -> KafkaJSONField {
        let fields = vec![
            KafkaJSONField::new(KafkaSchemaDataType::String, false, "connector_type", None),
            KafkaJSONField::new(KafkaSchemaDataType::String, false, "connector_name", None),
            KafkaJSONField::new(KafkaSchemaDataType::String, false, "db", None),
            KafkaJSONField::new(KafkaSchemaDataType::String, false, "table", None),
            KafkaJSONField::new(KafkaSchemaDataType::Int64, false, "server_id", None),
            KafkaJSONField::new(KafkaSchemaDataType::String, false, "file", None),
            KafkaJSONField::new(KafkaSchemaDataType::Int64, false, "pos", None),
        ];
        KafkaJSONField::new(KafkaSchemaDataType::Struct, false, "source", Some(fields))
    }
}
