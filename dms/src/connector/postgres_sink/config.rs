use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct PostgresSinkConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub tables: Vec<String>,
}
