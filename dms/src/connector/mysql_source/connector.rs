use crate::connector::connector::{KafkaMessageStream, SourceConnector};
use crate::connector::mysql_source::config::MySQLSourceConfig;
use crate::connector::mysql_source::decoder::MySQLDecoder;
use crate::error::error::DMSRResult;
use crate::kafka::kafka::Kafka;
use async_trait::async_trait;
use futures::StreamExt;
use log::debug;
use mysql_async::{BinlogRequest, Pool};

pub struct MySQLSourceConnector {
    config: MySQLSourceConfig,
    connector_name: String,
}

#[async_trait]
impl SourceConnector for MySQLSourceConnector {
    type Config = MySQLSourceConfig;

    async fn new(connector_name: String, config: &MySQLSourceConfig) -> DMSRResult<Box<Self>> {
        Ok(Box::new(MySQLSourceConnector {
            config: config.clone(),
            connector_name,
        }))
    }

    async fn cdc_events_to_stream(&mut self) -> DMSRResult<KafkaMessageStream> {
        let conn_str = format!(
            "mysql://{}:{}@{}:{}/{}",
            self.config.user,
            self.config.password,
            self.config.host,
            self.config.port,
            self.config.db
        );
        let pool = Pool::new(conn_str.as_str());
        let db_conn_binlog = pool.get_conn().await?;
        let binlog_request = BinlogRequest::new(self.config.server_id);
        let mut cdc_stream = db_conn_binlog.get_binlog_stream(binlog_request).await?;
        let mut decoder = MySQLDecoder::new(self.connector_name.clone(), self.config.clone());

        let stream: KafkaMessageStream = Box::pin(async_stream::stream! {
            while let Some(event) = cdc_stream.next().await {
                let event = event?;
                let kafka_messages = decoder.parse(event)?;
                for msg in kafka_messages {
                    yield Ok(msg);
                }
            }
        });

        Ok(stream)
    }

    async fn to_kafka(&self, kafka: &Kafka, stream: &mut KafkaMessageStream) -> DMSRResult<()> {
        while let Some(message) = stream.next().await {
            let message = message?;
            debug!("Sending message to kafka: {:?}", message);
            kafka.ingest(message).await?;
        }
        Ok(())
    }
}
