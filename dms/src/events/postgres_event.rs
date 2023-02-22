use serde::{Deserialize};

#[derive(Deserialize, Debug)]
pub struct PostgresEvent {
  change: Vec<Change>,
}

#[derive(Deserialize, Debug)]
pub struct Change {
  kind: String,
  schema: String,
  table: String,
  columnnames: Vec<String>,
  columntypes: Vec<String>,
  columnvalues: Vec<serde_json::Value>,
}