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

    pub fn get_schema(&self) -> Field {
        let fields = vec![
            Field::new("string".into(), false, "connector_type".into(), None),
            Field::new("string".into(), false, "connector_name".into(), None),
            Field::new("string".into(), false, "db".into(), None),
            Field::new("string".into(), false, "table".into(), None),
            Field::new("int64".into(), false, "server_id".into(), None),
            Field::new("string".into(), false, "file".into(), None),
            Field::new("int64".into(), false, "pos".into(), None),
        ];
        Field::new("struct".into(), false, "source".into(), Some(fields))
    }
}
