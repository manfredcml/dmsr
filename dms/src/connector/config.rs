use crate::connector::r#type::ConnectorType;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct ConnectorConfig {
    pub connector_type: ConnectorType,
    pub config: serde_json::Value,
}
