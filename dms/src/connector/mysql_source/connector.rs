use crate::connector::connector::{KafkaMessageStream, SourceConnector};
use crate::connector::mysql_source::config::MySQLSourceConfig;
use crate::connector::mysql_source::decoder::EventDecoder;
use crate::connector::mysql_source::metadata::MySQLSourceMetadata;
use crate::error::error::{DMSRError, DMSRResult};
use crate::kafka::kafka::Kafka;
use crate::kafka::payload::base::{Operation, Payload, PayloadEncoding};
use crate::kafka::payload::ddl_payload::DDLPayload;
use crate::kafka::payload::row_data_payload::RowDataPayload;
use async_trait::async_trait;
use futures::StreamExt;
use log::debug;
use mysql_async::prelude::Queryable;
use mysql_async::{BinlogRequest, Conn, Pool, Row};
use rdkafka::consumer::Consumer;
use serde_json::json;
use std::collections::HashMap;

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

        self.lock_tables(&mut conn).await?;
        let (binlog_name, binlog_pos) = self.read_binlog(&mut conn).await?;

        let schemas = self.read_schemas(&mut conn).await?;
        let tables = self.read_tables(&mut conn).await?;
        let columns = self.read_columns(&mut conn).await?;

        // Get DDLs
        let ddl_payloads = self
            .read_ddls(&mut conn, &binlog_name, &binlog_pos, &tables)
            .await?;

        // Reading data
        let r = self
            .read_table_data(&mut conn, &columns, &binlog_name, &binlog_pos)
            .await;
        println!("r: {:?}", r);

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
            kafka.produce(message).await?;
        }
        Ok(())
    }
}

impl MySQLSourceConnector {
    async fn lock_tables(&self, conn: &mut Conn) -> DMSRResult<()> {
        conn.query_drop("FLUSH TABLES WITH READ LOCK").await?;
        conn.query_drop("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .await?;
        conn.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
            .await?;
        Ok(())
    }

    async fn read_binlog(&self, conn: &mut Conn) -> DMSRResult<(String, u64)> {
        let query = "SHOW MASTER STATUS";
        let master_status: Vec<(String, u64, String, String, String)> = conn.query(query).await?;
        let binlog_name = &master_status[0].0;
        let binlog_name = binlog_name.to_string();
        let binlog_pos = master_status[0].1;

        Ok((binlog_name, binlog_pos))
    }

    async fn read_schemas(&self, conn: &mut Conn) -> DMSRResult<Vec<String>> {
        let query = r#"
            SELECT schema_name FROM information_schema.schemata
            WHERE schema_name
            NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
        "#;

        let schemas: Vec<String> = conn.query(query).await?;

        Ok(schemas)
    }

    async fn read_tables(&self, conn: &mut Conn) -> DMSRResult<Vec<(String, String)>> {
        let query = r#"
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema
            NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
        "#;

        let tables: Vec<(String, String)> = conn.query(query).await?;

        Ok(tables)
    }

    async fn read_columns(
        &self,
        conn: &mut Conn,
    ) -> DMSRResult<HashMap<(String, String), Vec<String>>> {
        let query = r#"
            SELECT table_schema, table_name, column_name
            FROM information_schema.columns
            WHERE table_schema
            NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
        "#;

        let columns: Vec<(String, String, String)> = conn.query(query).await?;

        let mut column_map = HashMap::new();
        for (schema, name, column_name) in columns {
            let key = (schema, name);
            let column_names = column_map.entry(key).or_insert(vec![]);
            column_names.push(column_name);
        }

        Ok(column_map)
    }

    async fn read_ddls(
        &self,
        conn: &mut Conn,
        binlog_name: &str,
        binlog_pos: &u64,
        tables: &[(String, String)],
    ) -> DMSRResult<Vec<DDLPayload<MySQLSourceMetadata>>> {
        let mut ddl_payloads: Vec<DDLPayload<MySQLSourceMetadata>> = vec![];

        for (schema, table) in tables.iter() {
            let metadata = MySQLSourceMetadata::new(
                &self.connector_name,
                &self.config.db,
                schema,
                table,
                self.config.server_id,
                binlog_name,
                *binlog_pos,
            );

            let ts_now = chrono::Utc::now().timestamp_millis() as u64;

            let mut res: Vec<(String, String)> = conn
                .query(format!("SHOW CREATE TABLE {}.{}", schema, table))
                .await?;

            let ddl = res
                .pop()
                .ok_or(DMSRError::MySQLSourceConnectorError("No DDL found".into()))?;

            let payload = DDLPayload::new(ddl.1, ts_now, metadata);

            ddl_payloads.push(payload)
        }

        conn.query_drop("UNLOCK TABLES").await?;

        Ok(ddl_payloads)
    }

    async fn read_table_data(
        &self,
        conn: &mut Conn,
        columns: &HashMap<(String, String), Vec<String>>,
        binlog_name: &str,
        binlog_pos: &u64,
    ) -> DMSRResult<()> {
        debug!("Reading table data...");

        for ((schema, table), column_names) in columns.iter() {
            let query = format!("SELECT * FROM {}.{}", schema, table);
            let rows: Vec<Row> = conn.query(query).await?;

            for row in rows {
                let mut row_data = json!({});
                for column_name in column_names {
                    let value: Option<String> = row.get(column_name.as_str()).ok_or(
                        DMSRError::MySQLSourceConnectorError("No value found".into()),
                    )?;
                    row_data[column_name] = json!(value);
                }

                let metadata = MySQLSourceMetadata::new(
                    &self.connector_name,
                    &self.config.db,
                    schema,
                    table,
                    self.config.server_id,
                    binlog_name,
                    *binlog_pos,
                );

                let ts_now = chrono::Utc::now().timestamp_millis() as u64;

                let row_data_payload: RowDataPayload<MySQLSourceMetadata> = RowDataPayload::new(
                    None,
                    Some(row_data),
                    Operation::Snapshot,
                    ts_now,
                    metadata,
                );

                // TODO: Send to Kafka
            }
        }

        Ok(())
    }
}
