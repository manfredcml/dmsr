use crate::connector::source_connector::SourceConnector;
use crate::connector::mysql_source::binlog_struct::MySQLTable;
use crate::connector::mysql_source::config::MySQLSourceConfig;
use crate::connector::output::OutputEncoding;
use crate::error::DMSRResult;
use crate::kafka::kafka_client::Kafka;
use async_trait::async_trait;
use futures::StreamExt;
use log::debug;
use mysql_async::binlog::events::TableMapEvent;
use mysql_async::Pool;
use std::collections::HashMap;

pub struct MySQLSourceConnector {
    pub(crate) config: MySQLSourceConfig,
    pub(crate) connector_name: String,
    pub(crate) pool: Pool,
    pub(crate) binlog_pos: Option<u64>,
    pub(crate) binlog_file_name: Option<String>,
    pub(crate) table_metadata_map: HashMap<String, MySQLTable>,
    pub(crate) last_table_map_event: Option<TableMapEvent<'static>>,
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
            binlog_pos: None,
            binlog_file_name: None,
            table_metadata_map: HashMap::new(),
            last_table_map_event: None,
        }))
    }

    async fn snapshot(&mut self, kafka: &Kafka) -> DMSRResult<()> {
        let (binlog_name, binlog_pos) = self.get_latest_binlog_info(kafka).await?;

        if !binlog_name.is_empty() {
            self.binlog_file_name = Some(binlog_name);
            self.binlog_pos = Some(binlog_pos);
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

    async fn stream(&mut self, kafka: &Kafka) -> DMSRResult<()> {
        let mut binlog_stream = self.get_binlog_stream().await?;

        debug!("Reading historical schema changes...");
        self.read_historical_schema_changes(kafka).await?;

        debug!("Creating Kafka message stream...");
        let kafka_config = kafka.config.clone();

        while let Some(event) = binlog_stream.next().await {
            debug!("Received event from MySQL: {:?}", event);
            let event = event?;
            let outputs = self.parse(event)?;
            for o in outputs {
                let message = o.to_kafka_message(&kafka_config, OutputEncoding::Default)?;
                kafka.produce(message).await?;
            }
        }

        Ok(())
    }
}
