use crate::connector::connector::Connector;
use crate::connector::mysql_source::config::MySQLSourceConfig;
use crate::connector::mysql_source::decoder::MySQLDecoder;
use crate::connector::mysql_source::table::MySQLTable;
use crate::error::error::{DMSRError, DMSRResult};
use crate::kafka::kafka::Kafka;
use crate::message::message::{KafkaMessage, Operation, Payload};
use crate::message::mysql_source::MySQLSource;
use async_trait::async_trait;
use futures::StreamExt;
use mysql_async::binlog::events::{Event, TableMapEvent};
use mysql_async::{BinlogRequest, BinlogStream, Pool, Value};
use std::collections::HashMap;

pub struct MySQLSourceConnector {
    config: MySQLSourceConfig,
    connector_name: String,
    cdc_stream: BinlogStream,
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

        Ok(Box::new(MySQLSourceConnector {
            config: config.clone(),
            connector_name,
            cdc_stream,
        }))
    }

    async fn stream(&mut self, kafka: &Kafka) -> DMSRResult<()> {
        let mut decoder = MySQLDecoder::new(self.connector_name.clone(), self.config.server_id);

        while let Some(event) = self.cdc_stream.next().await {
            let event = event?;
            let kafka_messages = decoder.parse(event)?;
            for msg in kafka_messages {
                println!("kafka_message: {:?}", msg);
            }
        }

        Ok(())
    }
}
