use crate::connector::kind::ConnectorKind;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct KafkaMessage<Source> {
    pub schema: Schema,
    pub payload: Payload<Source>,
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
pub struct Payload<Source> {
    pub before: Option<serde_json::Value>,
    pub after: Option<serde_json::Value>,
    pub op: Operation,
    pub ts_ms: i64,
    pub source: Source,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub enum Operation {
    #[serde(rename = "c")]
    Create,
    #[serde(rename = "u")]
    Update,
    #[serde(rename = "d")]
    Delete,
    #[serde(rename = "t")]
    Truncate,
}

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
