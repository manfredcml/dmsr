use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct KafkaMessage {
    pub schema: Schema,
    pub payload: Payload,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct Schema {
    pub r#type: String,
    pub fields: Vec<Field>,
    pub optional: bool,
    pub name: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct Field {
    pub r#type: String,
    pub optional: bool,
    pub field: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<Field>>,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct Payload {
    pub before: Option<serde_json::Value>,
    pub after: Option<serde_json::Value>,
    pub op: String,
    pub ts_ms: i64,
}
