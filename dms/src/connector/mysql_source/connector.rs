use crate::connector::connector::{KafkaMessageStream, SourceConnector};
use crate::connector::mysql_source::config::MySQLSourceConfig;
use crate::connector::mysql_source::decoder::EventDecoder;
use crate::connector::mysql_source::source_metadata::MySQLSourceMetadata;
use crate::error::error::{DMSRError, DMSRResult};
use crate::kafka::kafka::Kafka;
use crate::kafka::payload::base::{Operation, Payload, PayloadEncoding};
use crate::kafka::payload::ddl_payload::DDLPayload::MySQL;
use crate::kafka::payload::ddl_payload::{DDLPayload, MySQLDDLPayload};
use crate::kafka::payload::offset_payload::{MySQLOffsetPayload, OffsetPayload};
use crate::kafka::payload::row_data_payload::{MySQLRowDataPayload, RowDataPayload};
use async_trait::async_trait;
use futures::StreamExt;
use log::debug;
use mysql_async::prelude::Queryable;
use mysql_async::{BinlogRequest, Conn, Pool, Row};
use serde_json::{json, Value};
use std::collections::HashMap;

pub struct MySQLSourceConnector {
    config: MySQLSourceConfig,
    connector_name: String,
    pool: Pool,
    latest_binlog_pos: Option<u64>,
    latest_binlog_name: Option<String>,
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
            latest_binlog_pos: None,
            latest_binlog_name: None,
        }))
    }

    async fn snapshot(&mut self, kafka: &Kafka) -> DMSRResult<()> {
        let offset_topic_messages = kafka
            .poll_with_timeout(&kafka.config.offset_topic, 1)
            .await?;

        let mut connectors: HashMap<String, Value> = HashMap::new();
        for message in offset_topic_messages {
            let key_json: Value = serde_json::from_str(&message.key)?;
            if let Some(connector_name) = key_json.get("connector_name") {
                let connector_name = connector_name.as_str().unwrap_or_default();
                let value_json = serde_json::from_str(&message.value)?;
                connectors.insert(connector_name.to_string(), value_json);
            };
        }

        if let Some(binlog_info) = connectors.get(&self.connector_name) {
            let binlog_info = binlog_info.clone();
            let binlog_info: MySQLOffsetPayload = serde_json::from_value(binlog_info)?;
            self.latest_binlog_pos = Some(binlog_info.pos());
            self.latest_binlog_name = Some(binlog_info.file().to_string());
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

        conn.query_drop("UNLOCK TABLES").await?;

        self.read_table_data(kafka, &mut conn, &columns, &binlog_name, &binlog_pos)
            .await?;

        conn.query_drop("COMMIT").await?;
        conn.disconnect().await?;

        self.update_offsets(kafka, &binlog_name, &binlog_pos)
            .await?;

        Ok(())
    }

    async fn stream_messages(&mut self, kafka: &Kafka) -> DMSRResult<KafkaMessageStream> {
        let db_conn_binlog = self.pool.get_conn().await?;
        let mut binlog_request = BinlogRequest::new(self.config.server_id);

        if let (Some(binlog_pos), Some(binlog_name)) =
            (&self.latest_binlog_pos, &self.latest_binlog_name)
        {
            let binlog_name = binlog_name.as_bytes();
            binlog_request = binlog_request
                .with_filename(binlog_name)
                .with_pos(*binlog_pos);
        }

        let mut cdc_stream = db_conn_binlog.get_binlog_stream(binlog_request).await?;
        let mut decoder = EventDecoder::new(self.connector_name.clone(), self.config.clone());

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

            let value = serde_json::from_str::<MySQLDDLPayload>(&msg.value)?;

            decoder.parse_ddl_query(
                &schema,
                &value.ddl,
                value.ts_ms,
                value.metadata.pos,
            )?;
        }

        debug!("Creating Kafka message stream...");
        let kafka_config = kafka.config.clone();
        let stream: KafkaMessageStream = Box::pin(async_stream::stream! {
            while let Some(event) = cdc_stream.next().await {
                debug!("Received event from MySQL: {:?}", event);
                let event = event?;
                let payloads = decoder.parse(event)?;
                for p in payloads {
                    match p {
                        Payload::RowData(data) => {
                            let msg = data.to_kafka_message(&kafka_config, PayloadEncoding::Default)?;
                            yield Ok(msg);
                        }
                        Payload::DDL(data) => {
                            let msg = data.to_kafka_message(&kafka_config, PayloadEncoding::Default)?;
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
        kafka: &Kafka,
        conn: &mut Conn,
        binlog_name: &str,
        binlog_pos: &u64,
        tables: &[(String, String)],
    ) -> DMSRResult<()> {
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

            let payload = MySQLDDLPayload::new(&ddl.1, ts_now, metadata);
            let payload = MySQL(payload);
            let kafka_message =
                payload.to_kafka_message(&kafka.config, PayloadEncoding::Default)?;
            kafka.produce(kafka_message).await?;
        }

        Ok(())
    }

    async fn read_table_data(
        &self,
        kafka: &Kafka,
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

                let row_data_payload = MySQLRowDataPayload::new(
                    None,
                    Some(row_data),
                    Operation::Snapshot,
                    ts_now,
                    metadata,
                );

                let row_data_payload = RowDataPayload::MySQL(row_data_payload);

                let kafka_message =
                    row_data_payload.to_kafka_message(&kafka.config, PayloadEncoding::Default)?;

                kafka.produce(kafka_message).await?;
            }
        }

        Ok(())
    }

    async fn update_offsets(
        &self,
        kafka: &Kafka,
        binlog_name: &str,
        binlog_pos: &u64,
    ) -> DMSRResult<()> {
        let offset_payload = MySQLOffsetPayload::new(
            &self.connector_name,
            &self.config.db,
            self.config.server_id,
            binlog_name,
            *binlog_pos,
        );

        let offset_payload = OffsetPayload::MySQL(offset_payload);
        let kafka_message =
            offset_payload.to_kafka_message(&kafka.config, PayloadEncoding::Default)?;

        kafka.produce(kafka_message).await?;

        Ok(())
    }
}
