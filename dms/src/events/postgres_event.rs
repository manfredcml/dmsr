use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct PostgresEvent {
    pub schema: String,
    pub table: String,
}
