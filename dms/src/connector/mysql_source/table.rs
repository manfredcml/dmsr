use crate::connector::mysql_source::metadata::MySQLSourceMetadata;
use crate::error::error::{DMSRError, DMSRResult};
use crate::kafka::message::{KafkaJSONField, KafkaJSONSchema, KafkaMessage, KafkaSchemaDataType};

#[derive(Debug, Clone)]
pub struct MySQLTableColumn {
    pub column_name: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub is_primary_key: bool,
}

impl MySQLTableColumn {
    pub fn new(
        column_name: String,
        data_type: String,
        is_nullable: bool,
        is_primary_key: bool,
    ) -> Self {
        MySQLTableColumn {
            column_name,
            data_type,
            is_nullable,
            is_primary_key,
        }
    }

    pub fn to_kafka_connect_data_type(&self) -> DMSRResult<&str> {
        let split = self.data_type.split('(').collect::<Vec<&str>>();
        let data_type = split[0];

        let data_type = match data_type.to_lowercase().as_str() {
            "tinyint" => "int8",
            "boolean" => "boolean",
            "smallint" => "int16",
            "mediumint" => "int32",
            "int" => "int32",
            "integer" => "int32",
            "bigint" => "int64",
            "float" => "float32",
            "double" => "float64",
            "decimal" => "float64",
            "date" => "date",
            "time" => "time",
            "datetime" => "timestamp",
            "timestamp" => "timestamp",
            "year" => "int32",
            "char" => "string",
            "varchar" => "string",
            "tinytext" => "string",
            "text" => "string",
            "mediumtext" => "string",
            "longtext" => "string",
            "tinyblob" => "bytes",
            "blob" => "bytes",
            "mediumblob" => "bytes",
            "longblob" => "bytes",
            "binary" => "bytes",
            "varbinary" => "bytes",
            "enum" => "string",
            "set" => "string",
            "json" => "string",
            _ => {
                return Err(DMSRError::MySQLSourceConnectorError(
                    format!("Unknown data type: {}", data_type).into(),
                ))
            }
        };
        Ok(data_type)
    }
}

#[derive(Debug, Clone)]
pub struct MySQLTable {
    pub schema_name: String,
    pub table_name: String,
    pub columns: Vec<MySQLTableColumn>,
}

impl MySQLTable {
    pub fn column_as_mut(&mut self, column_name: &str) -> DMSRResult<&mut MySQLTableColumn> {
        let col = self
            .columns
            .iter_mut()
            .find(|c| c.column_name == column_name)
            .ok_or(DMSRError::MySQLSourceConnectorError(
                format!("Column {} not found", column_name).into(),
            ))?;
        Ok(col)
    }

    pub fn column_as_ref(&self, column_name: &str) -> DMSRResult<&MySQLTableColumn> {
        let col = self
            .columns
            .iter()
            .find(|c| c.column_name == column_name)
            .ok_or(DMSRError::MySQLSourceConnectorError(
                format!("Column {} not found", column_name).into(),
            ))?;
        Ok(col)
    }

    pub fn as_kafka_message_schema(&self, full_table_name: &str) -> DMSRResult<KafkaJSONSchema> {
        let mut fields = vec![];
        for col in &self.columns {
            let kafka_connect_data_type = col.to_kafka_connect_data_type()?;
            let field = KafkaJSONField::new(
                KafkaSchemaDataType::Struct,
                col.is_nullable,
                &col.column_name,
                None,
            );
            fields.push(field);
        }

        let before = KafkaJSONField::new(
            KafkaSchemaDataType::Struct,
            false,
            "before",
            Some(fields.clone()),
        );
        let after = KafkaJSONField::new(
            KafkaSchemaDataType::Struct,
            false,
            "after",
            Some(fields.clone()),
        );
        let source = MySQLSourceMetadata::get_schema();
        let schema = KafkaJSONSchema::new(before, after, source, full_table_name);
        Ok(schema)
    }
}
