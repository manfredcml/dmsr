use crate::connector::mysql_source::metadata::MySQLSourceMetadata;
use crate::error::error::DMSRResult;
use crate::kafka::message::KafkaMessage;
use rdkafka::metadata::Metadata;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct JSONMessage<Metadata>
where
    Metadata: serde::Serialize,
{
    pub schema: JSONSchema,
    pub payload: JSONPayload<Metadata>,
}

impl<JSONMetadata> JSONMessage<JSONMetadata>
where
    JSONMetadata: serde::Serialize,
{
    pub fn new(schema: JSONSchema, payload: JSONPayload<JSONMetadata>) -> Self {
        JSONMessage { schema, payload }
    }

    pub fn to_kafka_message(&self, topic: String, key: Option<String>) -> DMSRResult<KafkaMessage> {
        let value = serde_json::to_string(&self)?;
        Ok(KafkaMessage::new(topic, key, value))
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct JSONDDLMessage<Metadata>
where
    Metadata: serde::Serialize,
{
    pub schema: JSONSchema,
    pub payload: JSONDDLPayload<Metadata>,
}

impl<JSONMetadata> JSONDDLMessage<JSONMetadata>
where
    JSONMetadata: serde::Serialize,
{
    pub fn new(schema: JSONSchema, payload: JSONDDLPayload<JSONMetadata>) -> Self {
        JSONDDLMessage { schema, payload }
    }

    pub fn to_kafka_message(&self, topic: String, key: Option<String>) -> DMSRResult<KafkaMessage> {
        let value = serde_json::to_string(&self)?;
        Ok(KafkaMessage::new(topic, key, value))
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct JSONSchema {
    pub r#type: JSONDataType,
    pub fields: Vec<JSONSchemaField>,
    pub optional: bool,
    pub name: String,
}

impl JSONSchema {
    pub fn new(
        before: JSONSchemaField,
        after: JSONSchemaField,
        metadata: JSONSchemaField,
        name: &str,
    ) -> Self {
        let mut fields = vec![before, after, metadata];
        let mut other_fields = vec![
            JSONSchemaField::new(JSONDataType::String, false, "op", None),
            JSONSchemaField::new(JSONDataType::Int64, false, "ts_ms", None),
        ];
        fields.append(other_fields.as_mut());

        JSONSchema {
            r#type: JSONDataType::Struct,
            fields,
            optional: false,
            name: name.to_string(),
        }
    }

    pub fn new_ddl(metadata_schema: JSONSchemaField, name: &str) -> Self {
        let mut fields = vec![metadata_schema];
        let mut other_fields = vec![
            JSONSchemaField::new(JSONDataType::String, false, "ddl", None),
            JSONSchemaField::new(JSONDataType::Int64, false, "ts_ms", None),
        ];
        fields.append(other_fields.as_mut());

        JSONSchema {
            r#type: JSONDataType::Struct,
            fields,
            optional: false,
            name: name.to_string(),
        }
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct JSONSchemaField {
    pub r#type: JSONDataType,
    pub optional: bool,
    pub field: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<JSONSchemaField>>,
}

impl JSONSchemaField {
    pub fn new(
        r#type: JSONDataType,
        optional: bool,
        field: &str,
        fields: Option<Vec<JSONSchemaField>>,
    ) -> Self {
        JSONSchemaField {
            r#type,
            optional,
            field: field.to_string(),
            fields,
        }
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct JSONPayload<Metadata> {
    pub before: Option<serde_json::Value>,
    pub after: Option<serde_json::Value>,
    pub op: Operation,
    pub ts_ms: u64,
    pub metadata: Metadata,
}

impl<Metadata> JSONPayload<Metadata> {
    pub fn new(
        before: Option<serde_json::Value>,
        after: Option<serde_json::Value>,
        op: Operation,
        ts_ms: u64,
        metadata: Metadata,
    ) -> Self {
        JSONPayload {
            before,
            after,
            op,
            ts_ms,
            metadata,
        }
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct JSONDDLPayload<Metadata> {
    pub ddl: String,
    pub ts_ms: u64,
    pub metadata: Metadata,
}

impl<Metadata> JSONDDLPayload<Metadata> {
    pub fn new(ddl: &str, ts_ms: u64, metadata: Metadata) -> Self {
        JSONDDLPayload {
            ddl: ddl.to_string(),
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
pub enum JSONDataType {
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
