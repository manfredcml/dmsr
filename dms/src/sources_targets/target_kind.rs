use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub enum TargetKind {
    #[serde(rename = "postgres")]
    Postgres,
}
