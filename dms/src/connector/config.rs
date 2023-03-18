use serde::{Deserialize, Serialize};
use crate::connector::kind::ConnectorKind;

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct ConnectorConfig {
    pub connector_type: ConnectorKind,
    pub topic_prefix: String,
    pub config: serde_json::Value,
}
