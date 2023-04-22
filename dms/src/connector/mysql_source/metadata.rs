use crate::connector::kind::ConnectorKind;
use crate::kafka::json::{JSONDataType, JSONSchemaField};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct MySQLSourceMetadata {
    pub connector_type: ConnectorKind,
    pub connector_name: String,
    pub db: String,
    pub schema: Option<String>,
    pub table: String,
    pub server_id: u32,
    pub file: String,
    pub pos: u64,
}

impl MySQLSourceMetadata {
    pub fn new(
        connector_name: &str,
        db: &str,
        schema: Option<&str>,
        table: &str,
        server_id: u32,
        file: &str,
        pos: u64,
    ) -> Self {
        MySQLSourceMetadata {
            connector_type: ConnectorKind::MySQLSource,
            connector_name: connector_name.to_string(),
            db: db.to_string(),
            schema: schema.map(|s| s.to_string()),
            table: table.to_string(),
            server_id,
            file: file.to_string(),
            pos,
        }
    }

    pub fn get_schema() -> JSONSchemaField {
        let fields = vec![
            JSONSchemaField::new(JSONDataType::String, false, "connector_type", None),
            JSONSchemaField::new(JSONDataType::String, false, "connector_name", None),
            JSONSchemaField::new(JSONDataType::String, false, "db", None),
            JSONSchemaField::new(JSONDataType::String, true, "schema", None),
            JSONSchemaField::new(JSONDataType::String, false, "table", None),
            JSONSchemaField::new(JSONDataType::Int64, false, "server_id", None),
            JSONSchemaField::new(JSONDataType::String, false, "file", None),
            JSONSchemaField::new(JSONDataType::Int64, false, "pos", None),
        ];
        JSONSchemaField::new(JSONDataType::Struct, false, "source", Some(fields))
    }
}
