use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct PostgresEvent {
    pub schema: String,
    pub table: String,
    pub column_names: Vec<String>,
    pub column_types: Vec<String>,
    pub column_values: Vec<serde_json::Value>,
}
