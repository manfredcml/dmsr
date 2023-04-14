use crate::connector::connector::Connector;
use crate::connector::mysql_source::config::MySQLSourceConfig;
use crate::connector::postgres_source::event::{Action, Column, RawPostgresEvent};
use crate::error::error::{DMSRError, DMSRResult};
use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::Hash;
// use crate::event::message::{DataType, Field, JSONChangeEvent, Operation, Schema};
use crate::kafka::kafka::Kafka;
use async_trait::async_trait;
use byteorder::ReadBytesExt;
use futures::{future, ready, Sink, StreamExt};
use mysql_async::binlog::events::{BinlogEventHeader, Event, EventData};
use mysql_async::binlog::events::{QueryEvent, TableMapEvent, WriteRowsEvent, WriteRowsEventV1};
use mysql_async::binlog::EventType;
use mysql_async::prelude::Queryable;
use mysql_async::{BinlogRequest, BinlogStream, Conn, Pool};
use regex::Regex;
use std::io;
use std::io::{Cursor, Read};
use std::os::unix::raw::ino_t;
use std::sync::Arc;
use std::thread::spawn;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_postgres::{Client, CopyBothDuplex, NoTls, SimpleQueryMessage, SimpleQueryRow};

pub struct MySQLSourceConnector {
    config: MySQLSourceConfig,
    connector_name: String,
    db_conn: Arc<Mutex<Conn>>,
    cdc_stream: BinlogStream,
    table_map: HashMap<String, MySQLTable>,
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

        let mut db_conn_binlog = pool.get_conn().await?;
        let binlog_request = BinlogRequest::new(2);
        let mut cdc_stream = db_conn_binlog.get_binlog_stream(binlog_request).await?;

        let db_conn = pool.get_conn().await?;
        let db_conn = Arc::new(Mutex::new(db_conn));

        Ok(Box::new(MySQLSourceConnector {
            config: config.clone(),
            connector_name,
            db_conn,
            cdc_stream,
            table_map: HashMap::new(),
        }))
    }

    async fn stream(&mut self, kafka: &Kafka) -> DMSRResult<()> {
        while let Some(event) = self.cdc_stream.next().await {
            let event = event?;
            println!("Event: {:?}\n", event);

            let event_type = event.header().event_type()?;
            println!("Event type: {:?}\n", event_type);

            let data = event.data();
            println!("Data: {:?}\n", data);

            match event_type {
                EventType::QUERY_EVENT => {
                    let event = event.read_event::<QueryEvent>()?;
                    let schema = event.schema();
                    let query = event.query();
                    if let Ok(t) = self.get_table_info(schema.into(), query.into()).await {
                        let full_table_name = format!("{}.{}", t.schema_name, t.table_name);
                        self.table_map.insert(full_table_name, t);
                    }
                }
                EventType::TABLE_MAP_EVENT => {
                    let event = event.read_event::<TableMapEvent>()?;
                    let table_id = event.table_id();
                    let table_name = event.table_name();
                    let num_cols = event.columns_count();

                    let col_type_1 = event.get_column_type(0)?;
                    let col_type_2 = event.get_column_type(1)?;
                    let col_metadata_1 = event.get_column_metadata(0);
                    let col_metadata_2 = event.get_column_metadata(1);
                    println!("Table map event: {:?} \n", event);
                    println!("Table ID: {:?} \n", table_id);
                    println!("Table name: {:?} \n", table_name);
                    println!("Num cols: {:?} \n", num_cols);
                    println!("Col type 1: {:?} \n", col_type_1);
                    println!("Col type 2: {:?} \n", col_type_2);
                    println!("Col metadata 1: {:?} \n", col_metadata_1);
                    println!("Col metadata 2: {:?} \n", col_metadata_2);

                    for m in event.iter_optional_meta() {
                        println!("Optional meta: {:?} \n", m);
                    }
                }
                EventType::WRITE_ROWS_EVENT => {
                    let event = event.read_event::<WriteRowsEvent>()?;
                    let table_id = event.table_id();
                    let num_columns = event.num_columns();

                    println!(
                        "Table ID: {:?} - Num columns {:?} \n",
                        table_id, num_columns
                    );

                    let rows_data = event.rows_data();
                    println!("Rows data: {:?}\n", rows_data);
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

#[derive(Debug)]
struct MySQLTableColumn {
    column_name: String,
    ordinal_position: usize,
    data_type: String,
    is_nullable: bool,
    is_primary_key: bool,
}

#[derive(Debug)]
struct MySQLTable {
    schema_name: String,
    table_name: String,
    columns: Vec<MySQLTableColumn>,
}

impl MySQLSourceConnector {
    async fn get_table_info(&self, schema: String, query: String) -> DMSRResult<MySQLTable> {
        let pattern = Regex::new(r"(?i)(?:CREATE|ALTER)\s+TABLE\s+(?:\w+\.)?(\w+)")?;
        let table_name = pattern
            .captures(&query)
            .ok_or(DMSRError::MySQLError("Could not parse query".into()))?
            .get(1)
            .ok_or(DMSRError::MySQLError("Could not parse query".into()))?
            .as_str();

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
