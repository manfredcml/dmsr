use crate::connector::connector::{KafkaMessageStream, SourceConnector};
use crate::connector::mysql_source::config::MySQLSourceConfig;
use crate::connector::mysql_source::decoder::EventDecoder;
use crate::connector::mysql_source::metadata::MySQLSourceMetadata;
use crate::error::error::DMSRResult;
use crate::kafka::kafka::Kafka;
use crate::kafka::payload::base::{Payload, PayloadEncoding};
use async_trait::async_trait;
use futures::StreamExt;
use log::debug;
use mysql_async::prelude::Queryable;
use mysql_async::{BinlogRequest, Pool};
use rdkafka::consumer::Consumer;

pub struct MySQLSourceConnector {
    config: MySQLSourceConfig,
    connector_name: String,
    pool: Pool,
}

#[async_trait]
impl SourceConnector for MySQLSourceConnector {
    type Config = MySQLSourceConfig;
    type Metadata = MySQLSourceMetadata;

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
        }))
    }

    async fn snapshot(&self, kafka: &Kafka) -> DMSRResult<()> {
        // Global read lock
        let mut conn = self.pool.get_conn().await?;
        conn.query_drop("FLUSH TABLES WITH READ LOCK").await?;
        conn.query_drop("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .await?;
        conn.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
            .await?;

        let master_status: Vec<(String, u64, String, String, String)> =
            conn.query("SHOW MASTER STATUS").await?;
        let binlog_name = &master_status[0].0;
        let binlog_pos = &master_status[0].1;

        debug!("Binlog name: {}", binlog_name);
        debug!("Binlog pos: {}", binlog_pos);

        // Read schema
        let schemas: Vec<String> = conn
            .query("SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')")
            .await?;

        // Read tables
        let tables: Vec<(String, String)> = conn
            .query("SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')")
            .await?;

        let mut ddls: Vec<(String, String)> = vec![];
        for (schema, table) in tables.iter() {
            let mut res: Vec<(String, String)> = conn
                .query(format!("SHOW CREATE TABLE {}.{}", schema, table))
                .await?;
            if let Some(ddl) = res.pop() {
                ddls.push(ddl);
            }
        }

        debug!("DDLS: {:?}", ddls);

        conn.query_drop("UNLOCK TABLES").await?;
        conn.query_drop("COMMIT").await?;
        conn.disconnect().await?;

        // kafka.consumer.subscribe()
        Ok(())
    }

    async fn stream_messages(&mut self) -> DMSRResult<KafkaMessageStream> {
        let db_conn_binlog = self.pool.get_conn().await?;

        let binlog_request = BinlogRequest::new(self.config.server_id);
        let mut cdc_stream = db_conn_binlog.get_binlog_stream(binlog_request).await?;
        let mut decoder = EventDecoder::new(self.connector_name.clone(), self.config.clone());

        debug!("Creating Kafka message stream...");
        let stream: KafkaMessageStream = Box::pin(async_stream::stream! {
            while let Some(event) = cdc_stream.next().await {
                debug!("Received event from MySQL: {:?}", event);
                let event = event?;
                let payloads = decoder.parse(event)?;
                for p in payloads {
                    match p {
                        Payload::RowData(data) => {
                            let msg = data.into_kafka_message(PayloadEncoding::Default)?;
                            yield Ok(msg);
                        }
                        Payload::DDL(data) => {
                            let msg = data.into_kafka_message(PayloadEncoding::Default)?;
                            yield Ok(msg);
                        }
                    }
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
            kafka.ingest(message).await?;
        }
        Ok(())
    }
}
