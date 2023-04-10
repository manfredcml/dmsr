use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub struct AvroEvent {
    pub schema: Schema,
    pub payload: Payload,
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub struct Schema {
    pub r#type: String,
    pub fields: Vec<Field>,
    pub optional: bool,
    pub name: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub struct Field {
    pub r#type: String,
    pub optional: bool,
    pub name: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub struct Payload {
    pub before: Option<HashMap<String, serde_json::Value>>,
    pub after: Option<HashMap<String, serde_json::Value>>,
    pub op: String,
    pub ts_ms: i64,
}

// #[derive(Debug, Deserialize, Serialize, PartialEq)]
// pub struct JSONChangeEvent {
//     pub source_connector: String,
//     pub schema: Schema,
//     pub payload: HashMap<String, serde_json::Value>,
//     pub table: String,
//     pub op: Operation,
//     pub pk: Vec<String>,
// }
//
// #[derive(Debug, Deserialize, Serialize, PartialEq)]
// pub struct Schema {
//     pub r#type: DataType,
//     pub fields: Vec<Field>,
// }
//
// #[derive(Debug, Deserialize, Serialize, PartialEq)]
// pub struct Field {
//     pub r#type: DataType,
//     pub optional: bool,
//     pub field: String,
// }
//
// #[derive(Debug, Deserialize, Serialize, PartialEq)]
// pub enum DataType {
//     #[serde(rename = "struct")]
//     Struct,
//     #[serde(rename = "string")]
//     String,
//     #[serde(rename = "int8")]
//     Int8,
//     #[serde(rename = "int16")]
//     Int16,
//     #[serde(rename = "int32")]
//     Int32,
//     #[serde(rename = "int64")]
//     Int64,
//     #[serde(rename = "float32")]
//     Float32,
//     #[serde(rename = "float64")]
//     Float64,
//     #[serde(rename = "boolean")]
//     Boolean,
//     #[serde(rename = "bytes")]
//     Bytes,
//     #[serde(rename = "array")]
//     Array,
//     #[serde(rename = "map")]
//     Map,
// }
//
// #[derive(Debug, Deserialize, Serialize, PartialEq)]
// pub enum Operation {
//     #[serde(rename = "c")]
//     Create,
//     #[serde(rename = "u")]
//     Update,
//     #[serde(rename = "d")]
//     Delete,
// }
