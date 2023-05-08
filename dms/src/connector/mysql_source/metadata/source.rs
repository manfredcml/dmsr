use crate::connector::r#type::ConnectorType;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct MySQLSourceMetadata {
    pub connector_type: ConnectorType,
    pub connector_name: String,
    pub db: String,
    pub schema: String,
    pub table: String,
    pub server_id: u32,
    pub file: String,
    pub pos: u64,
}

impl MySQLSourceMetadata {
    pub fn new(
        connector_name: &str,
        db: &str,
        schema: &str,
        table: &str,
        server_id: u32,
        file: &str,
        pos: u64,
    ) -> Self {
        MySQLSourceMetadata {
            connector_type: ConnectorType::MySQLSource,
            connector_name: connector_name.to_string(),
            db: db.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            server_id,
            file: file.to_string(),
            pos,
        }
    }
}

impl MySQLSourceMetadata {
    pub fn kafka_topic(&self) -> String {
        format!("{}.{}.{}", self.connector_name, self.schema, self.table)
    }

    pub fn schema(&self) -> &str {
        &self.schema
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn connector_name(&self) -> &str {
        &self.connector_name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_mysql_source_metadata() {
        let metadata = MySQLSourceMetadata::new(
            "test_connector",
            "test_db",
            "test_schema",
            "test_table",
            1234,
            "test_file",
            5678,
        );
        assert_eq!(metadata.connector_type, ConnectorType::MySQLSource);
        assert_eq!(metadata.connector_name, "test_connector");
        assert_eq!(metadata.db, "test_db");
        assert_eq!(metadata.schema, "test_schema");
        assert_eq!(metadata.table, "test_table");
        assert_eq!(metadata.server_id, 1234);
        assert_eq!(metadata.file, "test_file");
        assert_eq!(metadata.pos, 5678);
    }

    #[test]
    fn test_clone_mysql_source_metadata() {
        let metadata = MySQLSourceMetadata::new(
            "test_connector",
            "test_db",
            "test_schema",
            "test_table",
            1234,
            "test_file",
            5678,
        );
        let cloned_metadata = metadata.clone();
        assert_eq!(metadata, cloned_metadata);
    }

    #[test]
    fn test_kafka_topic() {
        let metadata = MySQLSourceMetadata::new(
            "test_connector",
            "test_db",
            "test_schema",
            "test_table",
            1234,
            "test_file",
            5678,
        );
        let kafka_topic = metadata.kafka_topic();
        assert_eq!(kafka_topic, "test_connector.test_schema.test_table");
    }

    #[test]
    fn test_schema_and_table_methods() {
        let metadata = MySQLSourceMetadata::new(
            "test_connector",
            "test_db",
            "test_schema",
            "test_table",
            1234,
            "test_file",
            5678,
        );
        let schema = metadata.schema();
        let table = metadata.table();
        assert_eq!(schema, "test_schema");
        assert_eq!(table, "test_table");
    }

    #[test]
    fn test_connector_name_method() {
        let metadata = MySQLSourceMetadata::new(
            "test_connector",
            "test_db",
            "test_schema",
            "test_table",
            1234,
            "test_file",
            5678,
        );
        let connector_name = metadata.connector_name();
        assert_eq!(connector_name, "test_connector");
    }
}
