use crate::error::error::DMSRResult;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct KafkaMessage {
    pub topic: String,
    pub key: Option<String>,
    pub value: String,
}

impl KafkaMessage {
    pub fn new(topic: String, key: Option<String>, value: String) -> Self {
        KafkaMessage { topic, key, value }
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct KafkaJSONMessage<Metadata>
where
    Metadata: serde::Serialize,
{
    pub schema: KafkaJSONSchema,
    pub payload: KafkaJSONPayload<Metadata>,
}

impl<Metadata> KafkaJSONMessage<Metadata>
where
    Metadata: serde::Serialize,
{
    pub fn new(schema: KafkaJSONSchema, payload: KafkaJSONPayload<Metadata>) -> Self {
        KafkaJSONMessage { schema, payload }
    }

    pub fn to_kafka_message(&self, topic: String, key: Option<String>) -> DMSRResult<KafkaMessage> {
        let value = serde_json::to_string(&self)?;
        Ok(KafkaMessage::new(topic, key, value))
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct KafkaJSONSchema {
    pub r#type: KafkaSchemaDataType,
    pub fields: Vec<KafkaJSONField>,
    pub optional: bool,
    pub name: String,
}

impl KafkaJSONSchema {
    pub fn new<N>(
        before: KafkaJSONField,
        after: KafkaJSONField,
        metadata: KafkaJSONField,
        name: N,
    ) -> Self
    where
        N: Into<String>,
    {
        let mut fields = vec![before, after, metadata];
        let mut other_fields = vec![
            KafkaJSONField::new(KafkaSchemaDataType::String, false, "op", None),
            KafkaJSONField::new(KafkaSchemaDataType::Int64, false, "ts_ms", None),
        ];
        fields.append(other_fields.as_mut());

        KafkaJSONSchema {
            r#type: KafkaSchemaDataType::Struct,
            fields,
            optional: false,
            name: name.into(),
        }
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct KafkaJSONField {
    pub r#type: KafkaSchemaDataType,
    pub optional: bool,
    pub field: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<KafkaJSONField>>,
}

impl KafkaJSONField {
    pub fn new<F>(
        r#type: KafkaSchemaDataType,
        optional: bool,
        field: F,
        fields: Option<Vec<KafkaJSONField>>,
    ) -> Self
    where
        F: Into<String>,
    {
        KafkaJSONField {
            r#type,
            optional,
            field: field.into(),
            fields,
        }
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct KafkaJSONPayload<Metadata> {
    pub before: Option<serde_json::Value>,
    pub after: Option<serde_json::Value>,
    pub op: Operation,
    pub ts_ms: u64,
    pub metadata: Metadata,
}

impl<Metadata> KafkaJSONPayload<Metadata> {
    pub fn new(
        before: Option<serde_json::Value>,
        after: Option<serde_json::Value>,
        op: Operation,
        ts_ms: u64,
        metadata: Metadata,
    ) -> Self {
        KafkaJSONPayload {
            before,
            after,
            op,
            ts_ms,
            metadata,
        }
    }
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
pub enum KafkaSchemaDataType {
    #[serde(rename = "string")]
    String,
    #[serde(rename = "boolean")]
    Boolean,
    #[serde(rename = "bytes")]
    Bytes,
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
    #[serde(rename = "decimal")]
    Decimal,
    #[serde(rename = "date")]
    Date,
    #[serde(rename = "timestamp")]
    Timestamp,
    #[serde(rename = "struct")]
    Struct,
}
