use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub struct RawPostgresEvent {
    pub change: Vec<Change>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Change {
    pub kind: String,
    pub schema: String,
    pub table: String,
    #[serde(rename = "columnnames")]
    pub column_names: Vec<String>,
    #[serde(rename = "columntypes")]
    pub column_types: Vec<String>,
    #[serde(rename = "columnvalues")]
    pub column_values: Vec<serde_json::Value>,
}
