use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct MySQLSourceConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub db: String,
    pub server_id: u32,
}

impl MySQLSourceConfig {
    pub fn new(
        host: String,
        port: u16,
        user: String,
        password: String,
        db: String,
        server_id: u32,
    ) -> Self {
        MySQLSourceConfig {
            host,
            port,
            user,
            password,
            db,
            server_id,
        }
    }
}