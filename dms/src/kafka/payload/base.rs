use crate::error::error::DMSRResult;
use crate::kafka::message::KafkaMessage;
use crate::kafka::metadata::ConnectorMetadata;
use crate::kafka::payload::ddl_payload::DDLPayload;
use crate::kafka::payload::row_data_payload::RowDataPayload;
use serde::{Deserialize, Serialize};

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
    #[serde(rename = "r")]
    Snapshot,
}

pub enum Payload {
    RowData(RowDataPayload),
    DDL(DDLPayload),
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub enum PayloadEncoding {
    Default,
}
