use crate::connector::kind::ConnectorKind;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct PostgresSource {
    pub connector_type: ConnectorKind,
    pub connector_name: String,
    pub lsn: u64,
    pub db: String,
    pub schema: String,
    pub table: String,
    pub tx_id: u32,
}