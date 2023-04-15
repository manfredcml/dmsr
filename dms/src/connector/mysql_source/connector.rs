use crate::connector::connector::Connector;
use crate::connector::mysql_source::config::MySQLSourceConfig;
use crate::error::error::{DMSRError, DMSRResult};
use crate::kafka::kafka::Kafka;
use crate::types::mysql_types::{MySQLType, MySQLTypeMap};
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
            println!("Event: {:?}\n", event);

            let binlog_file = self.get_current_binlog_file().await?;
            let ts = event.fde().create_timestamp();
            let header = event.header();
            let log_pos = header.log_pos();

            let event_type = header.event_type()?;
            println!("Event type: {:?}\n", event_type);

            match event_type {
                EventType::QUERY_EVENT => {
                    let event = event.read_event::<QueryEvent>()?;
                    let schema = event.schema();
                    let query = event.query();
                    if let Ok(t) = self.get_table_info(schema.into(), query.into()).await {
                        let full_table_name = format!("{}.{}", t.schema_name, t.table_name);
                        self.table_name_map.insert(full_table_name, t);
                        println!("Table name map: {:?}", self.table_name_map);
                    }
                }
                EventType::TABLE_MAP_EVENT => {
                    let event = event.read_event::<TableMapEvent>()?;
                    let db_name = event.database_name();
                    let table_name = event.table_name();
                    println!("Database name: {:?}\n", db_name);
                    println!("Table name: {:?}\n", table_name);

                    table_event = Some(event.into_owned());
                }
                EventType::WRITE_ROWS_EVENT => {
                    let event = event.read_event::<WriteRowsEvent>()?;
                    let num_columns = event.num_columns() as usize;

                    let rows_data = event.rows_data();
                    println!("Rows data: {:?}\n", rows_data);

                    let table_event = table_event
                        .as_ref()
                        .ok_or(DMSRError::MySQLError("table event not found".into()))?;

                    let full_table_name = format!(
                        "{}.{}",
                        table_event.database_name(),
                        table_event.table_name()
                    );
                    println!("Full table name: {:?}\n", full_table_name);

                    let table_metadata = self.table_name_map.get(&full_table_name);

                    if table_metadata.is_none() {
                        continue;
                    }
                    let table_metadata = table_metadata.unwrap();

                    let table_columns = &table_metadata.columns;

                    println!("Table columns: {:?}\n", table_columns);
                    println!("Num columns: {:?}\n", num_columns);

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

                            println!("Value: {:?}\n", value);

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
                                BinlogValue::Jsonb(v) => {}
                                BinlogValue::JsonDiff(v) => {}
                            }

                        }
                        println!("Payload: {:?}\n", payload_value);
                    }
                }
                EventType::UPDATE_ROWS_EVENT => {
                    // let event = event.read_event::<WriteRowsEvent>()?;
                }
                EventType::DELETE_ROWS_EVENT => {
                    // let event = event.read_event::<WriteRowsEvent>()?;
                }
                _ => {}
            }

            println!("==========================================");

            // if let Some(e) = e {
            //     if let EventData::RowsEvent(e) = e {
            //         let rows_data = e.rows_data();
            //         let parsed = ParseBuf(rows_data);
            //         println!("Data: {:?}", parsed);
            //     }
            // }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct MySQLTableColumn {
    column_name: String,
    ordinal_position: usize,
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

impl MySQLSourceConnector {
    async fn get_current_binlog_file(&self) -> DMSRResult<String> {
        let mut db_conn = self.db_conn.lock().await;
        let res: Vec<(String, String, String, String, String)> =
            db_conn.query("SHOW MASTER STATUS").await?;
        Ok(res[0].0.clone())
    }

    async fn get_table_info(&self, schema: String, query: String) -> DMSRResult<MySQLTable> {
        println!("Schema: {:?}", schema);
        println!("Query: {:?}", query);
        let pattern = Regex::new(r"(?i)(?:CREATE|ALTER)\s+TABLE\s+(([\w_]+)\.)?([\w_]+)")?;
        let captures = pattern
            .captures(&query)
            .ok_or(DMSRError::MySQLError("Could not parse query".into()))?;

        let extracted_schema = captures.get(2).map(|m| m.as_str().to_string());
        let table_name = captures
            .get(3)
            .map(|m| m.as_str().to_string())
            .ok_or(DMSRError::MySQLError("Could not parse query".into()))?;

        let schema = extracted_schema.unwrap_or(schema);

        println!("Schema: {:?}", schema);
        println!("Table name: {:?}", table_name);

        let mut db_conn = self.db_conn.lock().await;
        let query = format!(
            "SELECT COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, IS_NULLABLE, COLUMN_KEY FROM information_schema.COLUMNS \
                WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}' ORDER BY ORDINAL_POSITION",
            schema, table_name
        );
        let res: Vec<(String, usize, String, String, String)> = db_conn.query(query).await?;
        let columns: Vec<MySQLTableColumn> = res
            .into_iter()
            .map(
                |(column_name, ordinal_position, data_type, is_nullable, column_key)| {
                    let is_nullable = is_nullable == "YES";
                    let is_primary_key = column_key == "PRI";
                    MySQLTableColumn {
                        column_name,
                        ordinal_position,
                        data_type,
                        is_nullable,
                        is_primary_key,
                    }
                },
            )
            .collect();

        let table = MySQLTable {
            schema_name: schema,
            table_name: table_name.into(),
            columns,
        };

        Ok(table)
    }
}
