use crate::connector::r#type::ConnectorType;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct PostgresSourceMetadata {
    pub connector_type: ConnectorType,
    pub connector_name: String,
    pub lsn: u64,
    pub db: String,
    pub schema: String,
    pub table: String,
    pub tx_id: u32,
}
