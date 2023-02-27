use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum SourceKind {
    #[serde(rename = "postgres")]
    Postgres,
}
