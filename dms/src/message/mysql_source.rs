use crate::connector::kind::ConnectorKind;
use crate::message::message::Field;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct MySQLSource {
    pub connector_type: ConnectorKind,
    pub connector_name: String,
    pub db: String,
    pub table: String,
    pub server_id: u32,
    pub file: String,
    pub pos: u64,
}

impl MySQLSource {
    pub fn new(
        connector_name: String,
        db: String,
        table: String,
        server_id: u32,
        file: String,
        pos: u64,
    ) -> Self {
        MySQLSource {
            connector_type: ConnectorKind::MySQLSource,
            connector_name,
            db,
            table,
            server_id,
            file,
            pos,
        }
    }

    pub fn get_schema() -> Field {
        let fields = vec![
            Field::new("string", false, "connector_type", None),
            Field::new("string", false, "connector_name", None),
            Field::new("string", false, "db", None),
            Field::new("string", false, "table", None),
            Field::new("int64", false, "server_id", None),
            Field::new("string", false, "file", None),
            Field::new("int64", false, "pos", None),
        ];
        Field::new("struct", false, "source", Some(fields))
    }
}
