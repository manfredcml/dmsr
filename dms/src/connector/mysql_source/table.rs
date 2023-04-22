use crate::connector::mysql_source::metadata::MySQLSourceMetadata;
use crate::error::error::{DMSRError, DMSRResult};
use crate::kafka::json::{JSONDataType, JSONSchema, JSONSchemaField};

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

    pub fn to_json_data_type(&self) -> DMSRResult<JSONDataType> {
        let split = self.data_type.split('(').collect::<Vec<&str>>();
        let data_type = split[0];

        let data_type = match data_type.to_lowercase().as_str() {
            "tinyint" => JSONDataType::Int8,
            "boolean" => JSONDataType::Boolean,
            "smallint" => JSONDataType::Int16,
            "mediumint" => JSONDataType::Int32,
            "int" => JSONDataType::Int32,
            "integer" => JSONDataType::Int32,
            "bigint" => JSONDataType::Int64,
            "float" => JSONDataType::Float32,
            "double" => JSONDataType::Float64,
            "decimal" => JSONDataType::Float64,
            "date" => JSONDataType::Date,
            "time" => JSONDataType::Timestamp,
            "datetime" => JSONDataType::Timestamp,
            "timestamp" => JSONDataType::Timestamp,
            "year" => JSONDataType::Int16,
            "char" => JSONDataType::String,
            "varchar" => JSONDataType::String,
            "tinytext" => JSONDataType::String,
            "text" => JSONDataType::String,
            "mediumtext" => JSONDataType::String,
            "longtext" => JSONDataType::String,
            "tinyblob" => JSONDataType::Bytes,
            "blob" => JSONDataType::Bytes,
            "mediumblob" => JSONDataType::Bytes,
            "longblob" => JSONDataType::Bytes,
            "binary" => JSONDataType::Bytes,
            "varbinary" => JSONDataType::Bytes,
            "enum" => JSONDataType::String,
            "set" => JSONDataType::String,
            "json" => JSONDataType::String,
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

    pub fn as_json_message_schema(&self, full_table_name: &str) -> DMSRResult<JSONSchema> {
        let mut fields = vec![];
        for col in &self.columns {
            let kafka_data_type = col.to_json_data_type()?;
            let field =
                JSONSchemaField::new(kafka_data_type, col.is_nullable, &col.column_name, None);
            fields.push(field);
        }

        let before =
            JSONSchemaField::new(JSONDataType::Struct, false, "before", Some(fields.clone()));
        let after =
            JSONSchemaField::new(JSONDataType::Struct, false, "after", Some(fields.clone()));
        let source = MySQLSourceMetadata::get_schema();
        let schema = JSONSchema::new(before, after, source, full_table_name);
        Ok(schema)
    }
}
