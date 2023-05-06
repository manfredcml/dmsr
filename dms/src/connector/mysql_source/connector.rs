use crate::connector::connector::{KafkaMessageStream, SourceConnector};
use crate::connector::mysql_source::config::MySQLSourceConfig;
use crate::connector::mysql_source::decoding::decoder::EventDecoder;
use crate::connector::mysql_source::output::ddl::MySQLDDLOutput;
use crate::connector::output::OutputEncoding;
use crate::error::{DMSRError, DMSRResult};
use crate::kafka::kafka::Kafka;
use async_trait::async_trait;
use futures::StreamExt;
use log::debug;
use mysql_async::{BinlogRequest, Pool};
use serde_json::Value;

pub struct MySQLSourceConnector {
    pub(crate) config: MySQLSourceConfig,
    pub(crate) connector_name: String,
    pub(crate) pool: Pool,
    pub(crate) latest_binlog_pos: Option<u64>,
    pub(crate) latest_binlog_name: Option<String>,
}

#[async_trait]
impl SourceConnector for MySQLSourceConnector {
    type Config = MySQLSourceConfig;

    async fn new(connector_name: String, config: MySQLSourceConfig) -> DMSRResult<Box<Self>> {
        let conn_str = format!(
            "mysql://{}:{}@{}:{}/{}",
            config.user, config.password, config.host, config.port, config.db
        );
        let pool = Pool::new(conn_str.as_str());
        Ok(Box::new(MySQLSourceConnector {
            config,
            connector_name,
            pool,
            latest_binlog_pos: None,
            latest_binlog_name: None,
        }))
    }

    async fn snapshot(&mut self, kafka: &Kafka) -> DMSRResult<()> {
        let (binlog_name, binlog_pos) = self.get_latest_binlog_info(kafka).await?;

        if !binlog_name.is_empty() {
            self.latest_binlog_name = Some(binlog_name);
            self.latest_binlog_pos = Some(binlog_pos);
            return Ok(());
        }

        let mut conn = self.pool.get_conn().await?;

        self.lock_tables(&mut conn).await?;
        let (binlog_name, binlog_pos) = self.read_binlog(&mut conn).await?;

        // let schemas = self.read_schemas(&mut conn).await?;
        let tables = self.read_tables(&mut conn).await?;
        let columns = self.read_columns(&mut conn).await?;

        self.read_ddls(kafka, &mut conn, &binlog_name, &binlog_pos, &tables)
            .await?;

        self.unlock_tables(&mut conn).await?;

        self.read_table_data(kafka, &mut conn, &columns, &binlog_name, &binlog_pos)
            .await?;

        self.finish_snapshot(conn).await?;

        self.update_offsets(kafka, &binlog_name, &binlog_pos)
            .await?;

        Ok(())
    }

    async fn stream_messages(&mut self, kafka: &Kafka) -> DMSRResult<KafkaMessageStream> {
        let mut decoder = EventDecoder::new(self.connector_name.clone(), self.config.clone());

        let db_conn_binlog = self.pool.get_conn().await?;
        let mut binlog_request = BinlogRequest::new(self.config.server_id);

        if let (Some(binlog_pos), Some(binlog_name)) =
            (&self.latest_binlog_pos, &self.latest_binlog_name)
        {
            decoder.set_binlog_file_name(binlog_name.clone());
            let binlog_name = binlog_name.as_bytes();
            binlog_request = binlog_request
                .with_filename(binlog_name)
                .with_pos(*binlog_pos);
        }

        let mut cdc_stream = db_conn_binlog.get_binlog_stream(binlog_request).await?;

        debug!("Reading historical schema changes...");
        let schema_messages = kafka.poll_with_timeout(&self.connector_name, 1).await?;

        for msg in schema_messages {
            let key_json = serde_json::from_str::<Value>(&msg.key)?;

            let schema = key_json
                .get("schema")
                .ok_or(DMSRError::MySQLSourceConnectorError(
                    "Schema not found in schema message".into(),
                ))?
                .as_str()
                .unwrap_or_default();

            let value = serde_json::from_str::<MySQLDDLOutput>(&msg.value)?;

            decoder.parse_ddl_query(schema, &value.ddl, value.ts_ms, value.metadata.pos)?;
        }

        debug!("Creating Kafka message stream...");
        let kafka_config = kafka.config.clone();
        let stream: KafkaMessageStream = Box::pin(async_stream::stream! {
            while let Some(event) = cdc_stream.next().await {
                debug!("Received event from MySQL: {:?}", event);
                let event = event?;
                let outputs = decoder.parse(event)?;
                for o in outputs {
                    let msg = o.to_kafka_message(&kafka_config, OutputEncoding::Default)?;
                    yield Ok(msg);
                }
            }
        });

        Ok(stream)
    }

    async fn publish_messages(
        &self,
        kafka: &Kafka,
        stream: &mut KafkaMessageStream,
    ) -> DMSRResult<()> {
        while let Some(message) = stream.next().await {
            debug!("Sending message to kafka: {:?}", message);
            let message = message?;
            kafka.produce(message).await?;
        }
        Ok(())
    }
}
