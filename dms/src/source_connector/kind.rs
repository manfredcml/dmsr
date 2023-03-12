use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum SourceConnectorKind {
    #[serde(rename = "postgres")]
    Postgres,
}
