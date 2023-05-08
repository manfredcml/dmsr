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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_mysql_source_config() {
        let config = MySQLSourceConfig::new(
            "localhost".to_string(),
            3306,
            "user".to_string(),
            "password".to_string(),
            "test_db".to_string(),
            1234,
        );
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 3306);
        assert_eq!(config.user, "user");
        assert_eq!(config.password, "password");
        assert_eq!(config.db, "test_db");
        assert_eq!(config.server_id, 1234);
    }

    #[test]
    fn test_clone_mysql_source_config() {
        let config = MySQLSourceConfig::new(
            "localhost".to_string(),
            3306,
            "user".to_string(),
            "password".to_string(),
            "test_db".to_string(),
            1234,
        );
        let cloned_config = config.clone();
        assert_eq!(config, cloned_config);
    }

    #[test]
    fn test_serialize_deserialize_mysql_source_config() {
        let config = MySQLSourceConfig::new(
            "localhost".to_string(),
            3306,
            "user".to_string(),
            "password".to_string(),
            "test_db".to_string(),
            1234,
        );
        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: MySQLSourceConfig = serde_json::from_str(&serialized).unwrap();
        assert_eq!(config, deserialized);
    }
}
