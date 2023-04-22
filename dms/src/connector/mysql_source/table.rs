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

    pub fn to_kafka_data_type(&self) -> DMSRResult<KafkaSchemaDataType> {
        let split = self.data_type.split('(').collect::<Vec<&str>>();
        let data_type = split[0];

        let data_type = match data_type.to_lowercase().as_str() {
            "tinyint" => KafkaSchemaDataType::Int8,
            "boolean" => KafkaSchemaDataType::Boolean,
            "smallint" => KafkaSchemaDataType::Int16,
            "mediumint" => KafkaSchemaDataType::Int32,
            "int" => KafkaSchemaDataType::Int32,
            "integer" => KafkaSchemaDataType::Int32,
            "bigint" => KafkaSchemaDataType::Int64,
            "float" => KafkaSchemaDataType::Float32,
            "double" => KafkaSchemaDataType::Float64,
            "decimal" => KafkaSchemaDataType::Float64,
            "date" => KafkaSchemaDataType::Date,
            "time" => KafkaSchemaDataType::Timestamp,
            "datetime" => KafkaSchemaDataType::Timestamp,
            "timestamp" => KafkaSchemaDataType::Timestamp,
            "year" => KafkaSchemaDataType::Int16,
            "char" => KafkaSchemaDataType::String,
            "varchar" => KafkaSchemaDataType::String,
            "tinytext" => KafkaSchemaDataType::String,
            "text" => KafkaSchemaDataType::String,
            "mediumtext" => KafkaSchemaDataType::String,
            "longtext" => KafkaSchemaDataType::String,
            "tinyblob" => KafkaSchemaDataType::Bytes,
            "blob" => KafkaSchemaDataType::Bytes,
            "mediumblob" => KafkaSchemaDataType::Bytes,
            "longblob" => KafkaSchemaDataType::Bytes,
            "binary" => KafkaSchemaDataType::Bytes,
            "varbinary" => KafkaSchemaDataType::Bytes,
            "enum" => KafkaSchemaDataType::String,
            "set" => KafkaSchemaDataType::String,
            "json" => KafkaSchemaDataType::String,
            _ => {
                return Err(DMSRError::MySQLSourceConnectorError(
                    format!("Unrecognized MySQL data type: {}", data_type).into(),
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
            let kafka_data_type = col.to_kafka_data_type()?;
            let field =
                KafkaJSONField::new(kafka_data_type, col.is_nullable, &col.column_name, None);
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
