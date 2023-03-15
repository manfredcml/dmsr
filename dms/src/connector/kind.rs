use crate::error::generic::DMSRError;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum ConnectorKind {
    #[serde(rename = "postgres_sink")]
    PostgresSink,

    #[serde(rename = "postgres_source")]
    PostgresSource,
}

impl FromStr for ConnectorKind {
    type Err = DMSRError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "postgres_sink" => Ok(ConnectorKind::PostgresSink),
            "postgres_source" => Ok(ConnectorKind::PostgresSource),
            _ => Err(DMSRError::UnknownConnectorError(s.to_string())),
        }
    }
}
