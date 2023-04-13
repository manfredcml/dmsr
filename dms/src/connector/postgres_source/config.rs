use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct PostgresSourceConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub db: String,
    pub password: String,
}
