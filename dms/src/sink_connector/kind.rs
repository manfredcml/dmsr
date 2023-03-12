use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum SinkConnectorKind {
    #[serde(rename = "postgres")]
    Postgres,
}
