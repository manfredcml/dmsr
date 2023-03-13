use crate::error::unknown_connector::UnknownConnectorError;
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
    type Err = UnknownConnectorError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "postgres_sink" => Ok(ConnectorKind::PostgresSink),
            "postgres_source" => Ok(ConnectorKind::PostgresSource),
            _ => {
                let err = UnknownConnectorError {
                    connector: s.to_string(),
                };
                Err(err)
            }
        }
    }
}
