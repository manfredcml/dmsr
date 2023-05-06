use crate::connector::r#type::ConnectorType;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct MySQLOffsetMetadata {
    pub connector_type: ConnectorType,
    pub connector_name: String,
    pub db: String,
    pub server_id: u32,
    pub file: String,
    pub pos: u64,
    #[serde(skip_serializing)]
    pub kafka_topic: String,
}

impl MySQLOffsetMetadata {
    pub fn new(
        connector_name: &str,
        db: &str,
        server_id: u32,
        file: &str,
        pos: u64,
        kafka_topic: &str,
    ) -> Self {
        MySQLOffsetMetadata {
            connector_type: ConnectorType::MySQLSource,
            connector_name: connector_name.to_string(),
            db: db.to_string(),
            server_id,
            file: file.to_string(),
            pos,
            kafka_topic: kafka_topic.to_string(),
        }
    }
}
