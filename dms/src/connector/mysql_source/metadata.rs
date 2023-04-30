use crate::connector::kind::ConnectorKind;
use crate::kafka::metadata::ConnectorMetadata;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct MySQLSourceMetadata {
    pub connector_type: ConnectorKind,
    pub connector_name: String,
    pub db: String,
    pub schema: String,
    pub table: String,
    pub server_id: u32,
    pub file: String,
    pub pos: u64,
}

impl MySQLSourceMetadata {
    pub fn new(
        connector_name: &str,
        db: &str,
        schema: &str,
        table: &str,
        server_id: u32,
        file: &str,
        pos: u64,
    ) -> Self {
        MySQLSourceMetadata {
            connector_type: ConnectorKind::MySQLSource,
            connector_name: connector_name.to_string(),
            db: db.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            server_id,
            file: file.to_string(),
            pos,
        }
    }
}

impl ConnectorMetadata for MySQLSourceMetadata {
    fn get_kafka_topic(&self) -> String {
        format!("{}.{}.{}", self.connector_name, self.schema, self.table)
    }

    fn get_schema(&self) -> &str {
        &self.schema
    }

    fn get_table(&self) -> &str {
        &self.table
    }
}
