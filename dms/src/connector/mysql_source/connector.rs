use crate::connector::connector::Connector;
use crate::connector::mysql_source::config::MySQLSourceConfig;
use crate::connector::postgres_source::event::{Action, Column, RawPostgresEvent};
use crate::error::error::{DMSRError, DMSRResult};
// use crate::event::message::{DataType, Field, JSONChangeEvent, Operation, Schema};
use crate::kafka::kafka::Kafka;
use async_trait::async_trait;
use byteorder::ReadBytesExt;
use futures::{future, ready, Sink, StreamExt};
use mysql_async::binlog::events::{BinlogEventHeader, Event, EventData};
use mysql_async::{BinlogRequest, BinlogStream, Conn, Pool};
use mysql_common::binlog::consts::EventType;
use mysql_common::binlog::events::{TableMapEvent, WriteRowsEvent, WriteRowsEventV1};
use mysql_common::io::ParseBuf;
use std::io;
use std::io::{Cursor, Read};
use std::os::unix::raw::ino_t;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_postgres::{Client, CopyBothDuplex, NoTls, SimpleQueryMessage, SimpleQueryRow};

pub struct MySQLSourceConnector {
    config: MySQLSourceConfig,
    stream: BinlogStream,
    connector_name: String,
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
        let conn = pool.get_conn().await?;
        let binlog_request = BinlogRequest::new(2);
        let stream = conn.get_binlog_stream(binlog_request).await?;

        Ok(Box::new(MySQLSourceConnector {
            config: config.clone(),
            stream,
            connector_name,
        }))
    }

    async fn stream(&mut self, kafka: &Kafka) -> DMSRResult<()> {
        while let Some(event) = self.stream.next().await {
            let event = event?;
            println!("Event: {:?}\n", event);

            let event_type = event.header().event_type()?;
            println!("Event type: {:?}\n", event_type);

            let data = event.data();
            println!("Data: {:?}\n", data);

            match event_type {
                EventType::TABLE_MAP_EVENT => {
                    let event = event.read_event::<TableMapEvent>()?;
                    let table_id = event.table_id();
                    let table_name = event.table_name();
                    let col_type_1 = event.get_column_type(0)?;
                    let col_type_2 = event.get_column_type(1)?;
                    println!("Table ID: {:?} \n", table_id);
                    println!("Table name: {:?} \n", table_name);
                    println!("Col type 1: {:?} \n", col_type_1);
                    println!("Col type 2: {:?} \n", col_type_2);
                }
                EventType::WRITE_ROWS_EVENT => {
                    let event = event.read_event::<WriteRowsEvent>()?;
                    let table_id = event.table_id();
                    let num_columns = event.num_columns();

                    println!("Table ID: {:?} - Num columns {:?} \n", table_id, num_columns);

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

pub fn read_bitmap_little_endian(
    cursor: &mut Cursor<&[u8]>,
    bits_number: usize,
) -> Result<Vec<bool>, io::Error> {
    let mut result = vec![false; bits_number];
    let bytes_number = (bits_number + 7) / 8;
    for i in 0..bytes_number {
        let value = cursor.read_u8()?;
        for y in 0..8 {
            let index = (i << 3) + y;
            if index == bits_number {
                break;
            }
            result[index] = (value & (1 << y)) > 0;
        }
    }
    Ok(result)
}
