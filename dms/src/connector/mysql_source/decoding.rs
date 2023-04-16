use crate::error::error::{DMSRError, DMSRResult};
use crate::message::message::{Field, Schema};
use mysql_async::binlog::events::TableMapEvent;
use sqlparser::ast::ObjectName;
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
    pub fn get_column_mut(&mut self, column_name: &str) -> DMSRResult<&mut MySQLTableColumn> {
        let col = self
            .columns
            .iter_mut()
            .find(|c| c.column_name == column_name)
            .ok_or(DMSRError::MySQLSourceConnectorError(
                format!("Column {} not found", column_name).into(),
            ))?;
        Ok(col)
    }

    pub fn get_column(&self, column_name: &str) -> DMSRResult<&MySQLTableColumn> {
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
            let field = Field::new(&col.data_type, !col.is_nullable, &col.column_name, None);
            fields.push(field);
        }

        let before = Field::new("struct", false, "before", Some(fields.clone()));
        let after = Field::new("struct", false, "after", Some(fields.clone()));
        let source = MySQLSource::get_schema();
        let schema = Schema::new(before, after, source, full_table_name);
        Ok(schema)
    }
}

pub fn parse_table_name_from_sqlparser_object_name(
    schema: &str,
    object_name: &ObjectName,
) -> DMSRResult<(String, String, String)> {
    let mut table_name = object_name.to_string();
    let split = table_name.split('.').collect::<Vec<&str>>();
    let mut schema = schema.to_string();
    if split.len() == 2 {
        schema = split[0].to_string();
        table_name = split[1].to_string();
    }
    let full_table_name = format!("{}.{}", schema, table_name);

    Ok((schema, table_name, full_table_name))
}

pub fn parse_table_name_from_table_map_event(
    event: &TableMapEvent,
) -> DMSRResult<(String, String, String)> {
    let mut schema = event.database_name().to_string();
    let mut table_name = event.table_name().to_string();

    let split = table_name.split('.').collect::<Vec<&str>>();
    if split.len() == 2 {
        schema = split[0].to_string();
        table_name = split[1].to_string();
    }
    let full_table_name = format!("{}.{}", schema, table_name);

    Ok((schema, table_name, full_table_name))
}
