use crate::error::DMSRError;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum ConnectorType {
    #[serde(rename = "postgres_source")]
    PostgresSource,

    #[serde(rename = "mysql_source")]
    MySQLSource,
}

impl FromStr for ConnectorType {
    type Err = DMSRError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "postgres_source" => Ok(ConnectorType::PostgresSource),
            "mysql_source" => Ok(ConnectorType::MySQLSource),
            _ => Err(DMSRError::UnknownConnectorError(s.to_string())),
        }
    }
}

impl ToString for ConnectorType {
    fn to_string(&self) -> String {
        match self {
            ConnectorType::PostgresSource => "postgres_source".to_string(),
            ConnectorType::MySQLSource => "mysql_source".to_string(),
        }
    }
}
