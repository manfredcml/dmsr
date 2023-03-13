use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum ConnectorKind {
    #[serde(rename = "postgres_sink")]
    PostgresSink,

    #[serde(rename = "postgres_source")]
    PostgresSource,
}
