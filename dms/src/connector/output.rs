use crate::connector::mysql_source::output::ddl::MySQLDDLOutput;
use crate::connector::mysql_source::output::offset::MySQLOffsetOutput;
use crate::connector::mysql_source::output::row_data::MySQLRowDataOutput;
use crate::error::DMSRResult;
use crate::kafka::config::KafkaConfig;
use crate::kafka::message::KafkaMessage;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub enum ConnectorOutput {
    MySQLRowData(MySQLRowDataOutput),
    MySQLDDL(MySQLDDLOutput),
    MySQLOffset(MySQLOffsetOutput),
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub enum OutputEncoding {
    Default,
}

impl ConnectorOutput {
    pub fn to_kafka_message(
        &self,
        config: &KafkaConfig,
        encoding: OutputEncoding,
    ) -> DMSRResult<KafkaMessage> {
        match self {
            ConnectorOutput::MySQLRowData(output) => match encoding {
                OutputEncoding::Default => output.to_kafka_message_default(config),
            },
            ConnectorOutput::MySQLDDL(output) => match encoding {
                OutputEncoding::Default => output.to_kafka_message_default(config),
            },
            ConnectorOutput::MySQLOffset(output) => match encoding {
                OutputEncoding::Default => output.to_kafka_message_default(config),
            },
        }
    }
}
