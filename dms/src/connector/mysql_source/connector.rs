use crate::connector::connector::Connector;
use crate::connector::mysql_source::config::MySQLSourceConfig;
use crate::connector::postgres_source::event::{Action, Column, RawPostgresEvent};
use crate::error::generic::{DMSRError, DMSRResult};
use crate::event::event::{DataType, Field, JSONChangeEvent, Operation, Schema};
use crate::kafka::kafka::Kafka;
use async_trait::async_trait;
use byteorder::ReadBytesExt;
use futures::{future, ready, Sink, StreamExt};
use mysql_async::binlog::events::{BinlogEventHeader, Event, EventData};
use mysql_async::{BinlogRequest, BinlogStream, Conn, Pool};
use mysql_common::binlog::events::{TableMapEvent, WriteRowsEvent, WriteRowsEventV1};
use mysql_common::io::ParseBuf;
use std::io;
use std::io::{Cursor, Read};
use std::os::unix::raw::ino_t;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_postgres::{Client, CopyBothDuplex, NoTls, SimpleQueryMessage, SimpleQueryRow};

pub struct MySQLSourceConnector {
    config: MySQLSourceConfig,
    stream: Option<BinlogStream>,
    connector_name: String,
}

#[async_trait]
impl Connector for MySQLSourceConnector {
    type Config = MySQLSourceConfig;

    fn new(connector_name: String, config: &MySQLSourceConfig) -> DMSRResult<Box<Self>> {
        Ok(Box::new(MySQLSourceConnector {
            config: config.clone(),
            stream: None,
            connector_name,
        }))
    }

    async fn connect(&mut self) -> DMSRResult<()> {
        let conn_str = format!(
            "mysql://{}:{}@{}:{}/{}",
            self.config.user,
            self.config.password,
            self.config.host,
            self.config.port,
            self.config.database
        );

        let pool = Pool::new(conn_str.as_str());
        let conn = pool.get_conn().await?;
        let binlog_request = BinlogRequest::new(2);
        let stream = conn.get_binlog_stream(binlog_request).await?;
        self.stream = Some(stream);
        Ok(())
    }

    async fn stream(&mut self, mut kafka: Kafka) -> DMSRResult<()> {
        let stream = match self.stream.as_mut() {
            Some(stream) => stream,
            None => {
                return Err(DMSRError::MissingValueError(
                    "Stream is not initialized".to_string(),
                ))
            }
        };

        while let Some(event) = stream.next().await {
            match event {
                Ok(event) => {
                    println!("Event: {:?}", event);

                    let event_type = event.header().event_type().unwrap();
                    println!("Event type: {:?}", event_type);

                    match event.read_event::<TableMapEvent>() {
                        Ok(e) => {
                            println!("TableMapEvent: {:?}", e);
                            println!("------------------------------");
                            let first = e.get_column_metadata(1);
                            let second = e.get_column_metadata(2);
                            let third = e.get_column_metadata(3);
                            println!("First column: {:?}", first);
                            println!("Second column: {:?}", second);
                            println!("==============================")
                        }
                        Err(e) => {
                            // continue
                        }
                    }

                    match event.read_event::<WriteRowsEventV1>() {
                        Ok(e) => {
                            println!("WriteRowsEventV1: {:?}", e);
                            println!("------------------------------");
                            let row_data = e.rows_data();
                            println!("Row data: {:?}", row_data);
                            println!("==============================")
                        }
                        Err(e) => {
                            // continue
                        }
                    }

                    match event.read_event::<WriteRowsEvent>() {
                        Ok(e) => {
                            println!("WriteRowsEvent: {:?}", e);
                            println!("------------------------------");
                            let row_data = e.rows_data();
                            println!("Row data: {:?}", row_data);
                            println!("==============================")
                        }
                        Err(e) => {
                            // continue
                        }
                    }

                    // if let Some(e) = e {
                    //     if let EventData::RowsEvent(e) = e {
                    //         let rows_data = e.rows_data();
                    //         let parsed = ParseBuf(rows_data);
                    //         println!("Data: {:?}", parsed);
                    //     }
                    // }
                }
                Err(e) => {
                    eprintln!("Error: {}", e);
                }
            }
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
