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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_str() {
        assert_eq!(
            ConnectorType::from_str("postgres_source").unwrap(),
            ConnectorType::PostgresSource
        );
        assert_eq!(
            ConnectorType::from_str("mysql_source").unwrap(),
            ConnectorType::MySQLSource
        );
        assert!(ConnectorType::from_str("unknown_source").is_err());
    }

    #[test]
    fn test_to_string() {
        assert_eq!(ConnectorType::PostgresSource.to_string(), "postgres_source");
        assert_eq!(ConnectorType::MySQLSource.to_string(), "mysql_source");
    }
}
