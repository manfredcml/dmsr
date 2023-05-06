use crate::connector::r#type::ConnectorType;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct MySQLSourceMetadata {
    pub connector_type: ConnectorType,
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
            connector_type: ConnectorType::MySQLSource,
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

impl MySQLSourceMetadata {
    pub fn kafka_topic(&self) -> String {
        format!("{}.{}.{}", self.connector_name, self.schema, self.table)
    }

    pub fn schema(&self) -> &str {
        &self.schema
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn connector_name(&self) -> &str {
        &self.connector_name
    }
}
