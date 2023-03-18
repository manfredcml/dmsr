use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct JSONChangeEvent {
    pub source_connector: String,
    pub schema: Schema,
    pub payload: HashMap<String, serde_json::Value>,
    pub table: String,
    pub op: Operation,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct Schema {
    pub r#type: DataType,
    pub fields: Vec<Field>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct Field {
    pub r#type: DataType,
    pub optional: bool,
    pub field: String,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub enum DataType {
    #[serde(rename = "struct")]
    Struct,
    #[serde(rename = "string")]
    String,
    #[serde(rename = "int8")]
    Int8,
    #[serde(rename = "int16")]
    Int16,
    #[serde(rename = "int32")]
    Int32,
    #[serde(rename = "int64")]
    Int64,
    #[serde(rename = "float32")]
    Float32,
    #[serde(rename = "float64")]
    Float64,
    #[serde(rename = "boolean")]
    Boolean,
    #[serde(rename = "bytes")]
    Bytes,
    #[serde(rename = "array")]
    Array,
    #[serde(rename = "map")]
    Map,
    None,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub enum Operation {
    #[serde(rename = "c")]
    Create,
    #[serde(rename = "u")]
    Update,
    #[serde(rename = "d")]
    Delete,
}
