use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct PostgresEvent {
    change: Vec<Change>,
}

#[derive(Deserialize, Debug)]
pub struct Change {
    kind: String,
    schema: String,
    table: String,
    #[serde(rename = "columnnames")]
    column_names: Vec<String>,
    #[serde(rename = "columntypes")]
    column_types: Vec<String>,
    #[serde(rename = "columnvalues")]
    column_values: Vec<serde_json::Value>,
}
