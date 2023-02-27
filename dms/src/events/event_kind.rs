use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub enum EventKind {
    #[serde(rename = "insert")]
    Insert,
    #[serde(rename = "update")]
    Update,
    #[serde(rename = "delete")]
    Delete,
}

impl FromStr for EventKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "insert" => Ok(EventKind::Insert),
            "update" => Ok(EventKind::Update),
            "delete" => Ok(EventKind::Delete),
            _ => Err(format!("Unknown event kind: {}", s)),
        }
    }
}
