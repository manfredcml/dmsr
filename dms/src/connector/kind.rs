use crate::error::error::DMSRError;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum ConnectorKind {
    #[serde(rename = "postgres_source")]
    PostgresSource,

    #[serde(rename = "mysql_source")]
    MySQLSource,
}

impl FromStr for ConnectorKind {
    type Err = DMSRError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "postgres_source" => Ok(ConnectorKind::PostgresSource),
            "mysql_source" => Ok(ConnectorKind::MySQLSource),
            _ => Err(DMSRError::UnknownConnectorError(s.to_string())),
        }
    }
}

impl ToString for ConnectorKind {
    fn to_string(&self) -> String {
        match self {
            ConnectorKind::PostgresSource => "postgres_source".to_string(),
            ConnectorKind::MySQLSource => "mysql_source".to_string(),
        }
    }
}
