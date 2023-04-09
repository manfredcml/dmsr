use crate::connector::kind::ConnectorKind;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct ConnectorConfig {
    pub connector_type: ConnectorKind,
    pub config: serde_json::Value,
}
