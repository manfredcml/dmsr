use crate::connector::connector::Connector;
use crate::connector::mysql_source::config::MySQLSourceConfig;
use crate::error::error::{DMSRError, DMSRResult};
use crate::kafka::kafka::Kafka;
use crate::types::mysql_types::MySQLTypeMap;
use async_trait::async_trait;
use byteorder::{BigEndian, LittleEndian, ReadBytesExt};
use futures::StreamExt;
use mysql_async::binlog::events::{QueryEvent, TableMapEvent, WriteRowsEvent};
use mysql_async::binlog::value::BinlogValue;
use mysql_async::binlog::EventType;
use mysql_async::prelude::Queryable;
use mysql_async::{BinlogRequest, BinlogStream, Conn, Pool, Value};
use regex::Regex;
use serde_json::json;
use sqlparser::ast::{
    AlterColumnOperation, AlterTableOperation, ColumnDef, ColumnOption, ColumnOptionDef,
    ObjectName, ObjectType, Statement, TableConstraint,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct MySQLSourceConnector {
    config: MySQLSourceConfig,
    connector_name: String,
    db_conn: Arc<Mutex<Conn>>,
    cdc_stream: BinlogStream,
    table_name_map: HashMap<String, MySQLTable>,
    table_id_map: HashMap<u64, MySQLTable>,
    kafka_connect_type_map: MySQLTypeMap,
}

#[async_trait]
impl Connector for MySQLSourceConnector {
    type Config = MySQLSourceConfig;

    async fn new(connector_name: String, config: &MySQLSourceConfig) -> DMSRResult<Box<Self>> {
        let conn_str = format!(
            "mysql://{}:{}@{}:{}/{}",
            config.user, config.password, config.host, config.port, config.db
        );
        let pool = Pool::new(conn_str.as_str());

        let db_conn_binlog = pool.get_conn().await?;
        let binlog_request = BinlogRequest::new(2);
        let cdc_stream = db_conn_binlog.get_binlog_stream(binlog_request).await?;

        let db_conn = pool.get_conn().await?;
        let db_conn = Arc::new(Mutex::new(db_conn));

        Ok(Box::new(MySQLSourceConnector {
            config: config.clone(),
            connector_name,
            db_conn,
            cdc_stream,
            table_name_map: HashMap::new(),
            table_id_map: HashMap::new(),
            kafka_connect_type_map: MySQLTypeMap::new(),
        }))
    }

    async fn stream(&mut self, kafka: &Kafka) -> DMSRResult<()> {
        let mut table_event: Option<TableMapEvent> = None;
        while let Some(event) = self.cdc_stream.next().await {
            let event = event?;

            let binlog_file = self.get_current_binlog_file().await?;
            let ts = event.fde().create_timestamp();
            let header = event.header();
            let log_pos = header.log_pos();

            let event_type = header.event_type()?;

            match event_type {
                EventType::QUERY_EVENT => {
                    let event = event.read_event::<QueryEvent>()?;
                    println!("Query event: {:?}\n", event);
                    let schema = event.schema();
                    let query = event.query();
                    self.get_table_info(schema.into(), query.into()).await?;
                    // if let Ok(t) = self.get_table_info(schema.into(), query.into()).await {
                    //     let full_table_name = format!("{}.{}", t.schema_name, t.table_name);
                    //     self.table_name_map.insert(full_table_name, t);
                    // }
                    println!("================================================")
                }
                EventType::TABLE_MAP_EVENT => {
                    let event = event.read_event::<TableMapEvent>()?;
                    let db_name = event.database_name();
                    let table_name = event.table_name();
                    table_event = Some(event.into_owned());
                }
                EventType::WRITE_ROWS_EVENT => {
                    let event = event.read_event::<WriteRowsEvent>()?;
                    let num_columns = event.num_columns() as usize;

                    let rows_data = event.rows_data();

                    let table_event = table_event
                        .as_ref()
                        .ok_or(DMSRError::MySQLError("table event not found".into()))?;

                    let full_table_name = format!(
                        "{}.{}",
                        table_event.database_name(),
                        table_event.table_name()
                    );

                    let table_metadata = self.table_name_map.get(&full_table_name);

                    if table_metadata.is_none() {
                        continue;
                    }
                    let table_metadata = table_metadata.unwrap();

                    let table_columns = &table_metadata.columns;

                    let binlog_rows = event.rows(table_event);
                    for row in binlog_rows {
                        let row = row?
                            .1
                            .ok_or(DMSRError::MySQLError("no column data found".into()))?;

                        let mut payload_value = json!({});
                        for n in 0..num_columns {
                            let value = row
                                .as_ref(n)
                                .ok_or(DMSRError::MySQLError("no column data found".into()))?;

                            let col = table_columns
                                .get(n)
                                .ok_or(DMSRError::MySQLError("no column metadata found".into()));

                            if col.is_err() {
                                break;
                            }
                            let col = col?;

                            match value {
                                BinlogValue::Value(Value::NULL) => {
                                    payload_value[col.column_name.as_str()] = json!(null);
                                }
                                BinlogValue::Value(v) => {
                                    let v = v.as_sql(false);
                                    payload_value[col.column_name.as_str()] = json!(v);
                                }
                                BinlogValue::Jsonb(v) => {}
                                BinlogValue::JsonDiff(v) => {}
                            }
                        }
                    }
                }
                EventType::UPDATE_ROWS_EVENT => {}
                EventType::DELETE_ROWS_EVENT => {}
                _ => {}
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct MySQLTableColumn {
    column_name: String,
    data_type: String,
    is_nullable: bool,
    is_primary_key: bool,
}

#[derive(Debug, Clone)]
struct MySQLTable {
    schema_name: String,
    table_name: String,
    columns: Vec<MySQLTableColumn>,
}

impl MySQLTable {
    pub fn find_column_mut(&mut self, column_name: &str) -> DMSRResult<&mut MySQLTableColumn> {
        let col = self
            .columns
            .iter_mut()
            .find(|c| c.column_name == column_name)
            .ok_or(DMSRError::MySQLError(format!(
                "column {} not found",
                column_name
            )))?;
        Ok(col)
    }

    pub fn find_column(&self, column_name: &str) -> DMSRResult<&MySQLTableColumn> {
        let col = self
            .columns
            .iter()
            .find(|c| c.column_name == column_name)
            .ok_or(DMSRError::MySQLError(format!(
                "column {} not found",
                column_name
            )))?;
        Ok(col)
    }
}

impl MySQLSourceConnector {
    async fn get_current_binlog_file(&self) -> DMSRResult<String> {
        let mut db_conn = self.db_conn.lock().await;
        let res: Vec<(String, String, String, String, String)> =
            db_conn.query("SHOW MASTER STATUS").await?;
        Ok(res[0].0.clone())
    }

    async fn get_table_info(&mut self, schema: String, query: String) -> DMSRResult<()> {
        println!("Query: {:?}\n", query);

        let dialect = MySqlDialect {};
        let ast: Vec<Statement> = Parser::parse_sql(&dialect, query.as_str())?;

        println!("AST: {:?}\n", ast.first());

        match ast.first() {
            Some(Statement::CreateTable {
                name,
                columns,
                constraints,
                ..
            }) => {
                let (schema, table_name, full_table_name) = self.parse_table_name(&schema, name)?;
                let mut mysql_table_columns: Vec<MySQLTableColumn> = vec![];

                for c in columns {
                    let column_name = c.name.to_string();
                    let data_type = c.data_type.to_string();

                    let is_nullable = !c
                        .options
                        .iter()
                        .map(|def| &def.option)
                        .any(|o| matches!(o, ColumnOption::NotNull));

                    let is_primary_key = c.options.iter().map(|def| &def.option).any(|o| match o {
                        ColumnOption::Unique { is_primary } => *is_primary,
                        _ => false,
                    });

                    mysql_table_columns.push(MySQLTableColumn {
                        column_name,
                        data_type,
                        is_nullable: is_nullable && !is_primary_key,
                        is_primary_key,
                    });
                }

                let constraints = constraints as &Vec<TableConstraint>;
                for c in constraints {
                    if let TableConstraint::Unique {
                        columns,
                        is_primary,
                        ..
                    } = c
                    {
                        if !is_primary {
                            continue;
                        }
                        for col in columns {
                            let col = col.to_string();
                            let col = mysql_table_columns
                                .iter_mut()
                                .find(|c| c.column_name == col);
                            if col.is_none() {
                                continue;
                            }
                            let col = col.unwrap();
                            col.is_nullable = false;
                            col.is_primary_key = true;
                        }
                    }
                }

                let mysql_table = MySQLTable {
                    schema_name: schema,
                    table_name,
                    columns: mysql_table_columns,
                };

                println!("MySQL Table: {:?}\n", mysql_table);

                self.table_name_map.insert(full_table_name, mysql_table);
            }
            Some(Statement::AlterTable { name, operation }) => {
                let (schema, table_name, full_table_name) = self.parse_table_name(&schema, name)?;

                println!("Operation: {:?}\n", operation);

                match operation {
                    AlterTableOperation::AddColumn { column_def, .. } => {
                        let column_name = column_def.name.to_string();
                        let data_type = column_def.data_type.to_string();
                        let is_nullable = !column_def
                            .options
                            .iter()
                            .map(|def| &def.option)
                            .any(|o| matches!(o, ColumnOption::NotNull));

                        let is_primary_key =
                            column_def
                                .options
                                .iter()
                                .map(|def| &def.option)
                                .any(|o| match o {
                                    ColumnOption::Unique { is_primary } => *is_primary,
                                    _ => false,
                                });

                        let table = self.retrieve_table_meta_mut(&full_table_name)?;
                        table.columns.push(MySQLTableColumn {
                            column_name,
                            data_type,
                            is_nullable: is_nullable && !is_primary_key,
                            is_primary_key,
                        });
                    }
                    AlterTableOperation::AlterColumn { column_name, op } => {
                        let column_name = column_name.to_string();
                        let col = self
                            .retrieve_table_meta_mut(&full_table_name)?
                            .find_column_mut(&column_name)?;

                        match op {
                            AlterColumnOperation::SetDataType { data_type, .. } => {
                                col.data_type = data_type.to_string();
                            }
                            AlterColumnOperation::SetNotNull => {
                                col.is_nullable = false;
                            }
                            AlterColumnOperation::DropNotNull => {
                                col.is_nullable = true;
                            }
                            _ => {}
                        }
                    }
                    AlterTableOperation::DropColumn { column_name, .. } => {
                        let column_name = column_name.to_string();
                        let table = self.retrieve_table_meta_mut(&full_table_name)?;
                        table.columns = table
                            .columns
                            .clone()
                            .into_iter()
                            .filter(|c| c.column_name != column_name)
                            .collect();
                    }
                    AlterTableOperation::DropPrimaryKey => {
                        let table = self.retrieve_table_meta_mut(&full_table_name)?;
                        table.columns = table
                            .columns
                            .clone()
                            .into_iter()
                            .map(|mut c| {
                                c.is_primary_key = false;
                                c
                            })
                            .collect();
                    }
                    AlterTableOperation::RenameColumn {
                        old_column_name,
                        new_column_name,
                    } => {
                        let table = self.retrieve_table_meta_mut(&full_table_name)?;
                        let old_column_name = old_column_name.to_string();
                        let new_column_name = new_column_name.to_string();
                        let col = table.find_column_mut(&old_column_name)?;
                        col.column_name = new_column_name;
                    }
                    AlterTableOperation::RenameTable { table_name } => {
                        let (_, _, new_full_table_name) = self.parse_table_name(&schema, table_name)?;
                        let mut table = self.retrieve_table_meta(&full_table_name)?.clone();
                        table.table_name = table_name.to_string();
                        self.table_name_map.insert(new_full_table_name, table);
                        self.table_name_map.remove(&full_table_name);
                    }
                    _ => {}
                }
            }
            _ => {}
        }

        Ok(())
    }

    fn parse_table_name(
        &self,
        schema: &String,
        object_name: &ObjectName,
    ) -> DMSRResult<(String, String, String)> {
        let mut table_name = object_name.to_string();
        let split = table_name.split('.').collect::<Vec<&str>>();
        let mut schema = schema.clone();
        if split.len() == 2 {
            schema = split[0].to_string();
            table_name = split[1].to_string();
        }
        let full_table_name = format!("{}.{}", schema, table_name);

        Ok((schema, table_name, full_table_name))
    }

    fn retrieve_table_meta_mut(&mut self, table_name: &String) -> DMSRResult<&mut MySQLTable> {
        self.table_name_map
            .get_mut(table_name)
            .ok_or(DMSRError::MySQLSourceConnectorError(
                "No table metadata found".into(),
            ))
    }

    fn retrieve_table_meta(&self, table_name: &String) -> DMSRResult<&MySQLTable> {
        self.table_name_map
            .get(table_name)
            .ok_or(DMSRError::MySQLSourceConnectorError(
                "No table metadata found".into(),
            ))
    }
}
