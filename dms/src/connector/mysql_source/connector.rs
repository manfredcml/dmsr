use crate::connector::connector::{KafkaMessageStream, SourceConnector};
use crate::connector::mysql_source::config::MySQLSourceConfig;
use crate::connector::mysql_source::decoder::MySQLDecoder;
use crate::connector::mysql_source::metadata::MySQLSourceMetadata;
use crate::error::error::DMSRResult;
use crate::kafka::kafka::Kafka;
use crate::kafka::message::{KafkaJSONMessage, KafkaMessage};
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use log::error;
use mysql_async::{BinlogRequest, BinlogStream, Pool};
use std::pin::Pin;

pub struct MySQLSourceConnector {
    config: MySQLSourceConfig,
    connector_name: String,
    cdc_stream: BinlogStream,
}

#[async_trait]
impl SourceConnector for MySQLSourceConnector {
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

    async fn cdc_events_to_stream(&mut self) -> DMSRResult<KafkaMessageStream> {
        let mut decoder = MySQLDecoder::new(&self.connector_name, &self.config);

        let stream = futures::stream::unfold(decoder, |mut decoder| async move {
            let event = match self.cdc_stream.next().await {
                Some(Ok(event)) => event,
                Some(Err(err)) => return Some((Err(err.into()), decoder)),
                None => return None,
            };

            let messages = match decoder.parse(event) {
                Ok(kafka_messages) => kafka_messages,
                Err(err) => return Some((Err(err), decoder)),
            };

            Some((Ok(messages), decoder))
        });

        let stream = stream
            .filter_map(|result| async move {
                result.ok()
            })
            .flat_map(|kafka_messages| futures::stream::iter(kafka_messages));

        Ok(Box::pin(stream))
    }

    async fn to_kafka(&self, kafka: &Kafka, stream: &mut KafkaMessageStream) -> DMSRResult<()> {
        while let Some(message) = stream.next().await {
            kafka.ingest(message).await?;
        }
        Ok(())
    }
}
