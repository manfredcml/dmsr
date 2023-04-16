use crate::connector::connector::Connector;
use crate::connector::mysql_source::config::MySQLSourceConfig;
use crate::connector::mysql_source::decoding::{
    parse_table_name_from_sqlparser_object_name, parse_table_name_from_table_map_event, MySQLTable,
    MySQLTableColumn,
};
use crate::error::error::{DMSRError, DMSRResult};
use crate::kafka::kafka::Kafka;
use crate::message::message::{KafkaMessage, Operation, Payload, Schema};
use crate::message::mysql_source::MySQLSource;
use crate::types::mysql_types::MySQLTypeMap;
use async_trait::async_trait;
use byteorder::{BigEndian, LittleEndian, ReadBytesExt};
use futures::StreamExt;
use mysql_async::binlog::events::{QueryEvent, TableMapEvent, WriteRowsEvent};
use mysql_async::binlog::value::BinlogValue;
use mysql_async::binlog::EventType;
use mysql_async::{BinlogRequest, BinlogStream, Conn, Pool, Value};
use serde_json::json;
use sqlparser::ast::{
    AlterColumnOperation, AlterTableOperation, ColumnDef, ColumnOption, ColumnOptionDef,
    ObjectName, ObjectType, Statement, Table, TableConstraint,
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
    table_def_map: HashMap<String, MySQLTable>,
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
            table_def_map: HashMap::new(),
            kafka_connect_type_map: MySQLTypeMap::new(),
        }))
    }

    async fn stream(&mut self, kafka: &Kafka) -> DMSRResult<()> {
        let mut table_event: Option<TableMapEvent> = None;
        while let Some(event) = self.cdc_stream.next().await {
            let event = event?;

            let ts = event.fde().create_timestamp();
            let header = event.header();
            let log_pos = header.log_pos() as u64;

            let event_type = header.event_type()?;

            match event_type {
                EventType::QUERY_EVENT => {
                    let event = event.read_event::<QueryEvent>()?;
                    println!("QUERY_EVENT: {:?}\n", event);
                    let schema = event.schema();
                    let query = event.query();
                    self.update_table_def(schema.into(), query.into()).await?;
                }
                EventType::TABLE_MAP_EVENT => {
                    let event = event.read_event::<TableMapEvent>()?;
                    println!("TABLE_MAP_EVENT: {:?}\n", event);
                    table_event = Some(event.into_owned());
                }
                EventType::WRITE_ROWS_EVENT => {
                    let event = event.read_event::<WriteRowsEvent>()?;
                    println!("WRITE_ROWS_EVENT: {:?}\n", event);

                    let table_event =
                        table_event
                            .as_ref()
                            .ok_or(DMSRError::MySQLSourceConnectorError(
                                "Preceding TABLE_MAP_EVENT not found".into(),
                            ))?;

                    let (schema, table_name, full_table_name) =
                        parse_table_name_from_table_map_event(table_event)?;

                    println!("full_table_name: {:?}\n", full_table_name);
                    let table_metadata = self.retrieve_table_meta(&full_table_name);
                    let Ok(table_metadata) = table_metadata else {
                        continue;
                    };
                    println!("table_metadata: {:?}\n", table_metadata);

                    let table_columns = &table_metadata.columns;

                    let num_columns = event.num_columns() as usize;

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
                                .ok_or(DMSRError::MySQLError("no column metadata found".into()))?;

                            match value {
                                BinlogValue::Value(Value::NULL) => {
                                    payload_value[col.column_name.as_str()] = json!(null);
                                }
                                BinlogValue::Value(v) => {
                                    let v = v.as_sql(false);
                                    payload_value[col.column_name.as_str()] = json!(v);
                                }
                                BinlogValue::Jsonb(_) => {}
                                BinlogValue::JsonDiff(_) => {}
                            }
                        }

                        let kafka_message_schema =
                            table_metadata.as_kafka_message_schema(full_table_name.as_str())?;

                        let kafka_message_source = MySQLSource::new(
                            self.connector_name.clone(),
                            schema.clone(),
                            table_name.clone(),
                            0,
                            "PLACEHOLDER".into(),
                            log_pos,
                        );

                        let kafka_message_payload: Payload<MySQLSource> = Payload::new(
                            None,
                            Some(payload_value),
                            Operation::Create,
                            0,
                            kafka_message_source,
                        );

                        let kafka_message =
                            KafkaMessage::new(kafka_message_schema, kafka_message_payload);

                        println!("Kafka message: {:?}", serde_json::to_string(&kafka_message)?);
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

impl MySQLSourceConnector {
    async fn update_table_def(&mut self, schema: String, query: String) -> DMSRResult<()> {
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
                let (schema, table_name, full_table_name) =
                    parse_table_name_from_sqlparser_object_name(&schema, name)?;
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

                self.table_def_map.insert(full_table_name, mysql_table);
            }
            Some(Statement::AlterTable { name, operation }) => {
                let (schema, _, full_table_name) =
                    parse_table_name_from_sqlparser_object_name(&schema, name)?;

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
                            .get_column_mut(&column_name)?;

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
                        let col = table.get_column_mut(&old_column_name)?;
                        col.column_name = new_column_name;
                    }
                    AlterTableOperation::RenameTable { table_name } => {
                        let (_, _, new_full_table_name) =
                            parse_table_name_from_sqlparser_object_name(&schema, table_name)?;
                        let mut table = self.retrieve_table_meta(&full_table_name)?.clone();
                        table.table_name = table_name.to_string();
                        self.table_def_map.insert(new_full_table_name, table);
                        self.table_def_map.remove(&full_table_name);
                    }
                    _ => {}
                }
            }
            _ => {}
        }

        Ok(())
    }

    fn retrieve_table_meta_mut(&mut self, table_name: &String) -> DMSRResult<&mut MySQLTable> {
        self.table_def_map
            .get_mut(table_name)
            .ok_or(DMSRError::MySQLSourceConnectorError(
                "No table metadata found".into(),
            ))
    }

    fn retrieve_table_meta(&self, table_name: &String) -> DMSRResult<&MySQLTable> {
        self.table_def_map
            .get(table_name)
            .ok_or(DMSRError::MySQLSourceConnectorError(
                "No table metadata found".into(),
            ))
    }
}
