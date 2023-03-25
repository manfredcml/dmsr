use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub enum Action {
    B,
    I,
    D,
    U,
    C,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct RawPostgresEvent {
    pub action: Action,
    pub schema: Option<String>,
    pub table: Option<String>,
    pub columns: Option<Vec<Column>>,
    pub identity: Option<Vec<Column>>,
    pub pk: Option<Vec<PrimaryKey>>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Column {
    pub name: String,
    pub r#type: String,
    pub value: serde_json::Value,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct PrimaryKey {
    pub name: String,
    pub r#type: String,
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
