use crate::error::error::{DMSRError, DMSRResult};
use crate::message::message::{Field, Schema};
use crate::message::mysql_source::MySQLSource;

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

    pub fn as_kafka_message_schema(&self, full_table_name: &str) -> DMSRResult<Schema> {
        let mut fields = vec![];
        for col in &self.columns {
            let field = Field::new(&col.data_type, col.is_nullable, &col.column_name, None);
            fields.push(field);
        }

        let before = Field::new("struct", false, "before", Some(fields.clone()));
        let after = Field::new("struct", false, "after", Some(fields.clone()));
        let source = MySQLSource::get_schema();
        let schema = Schema::new(before, after, source, full_table_name);
        Ok(schema)
    }
}
