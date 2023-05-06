use crate::connector::mysql_source::connector::MySQLSourceConnector;
use crate::connector::mysql_source::metadata::source::MySQLSourceMetadata;
use crate::connector::mysql_source::output::ddl::MySQLDDLOutput;
use crate::connector::mysql_source::output::offset::MySQLOffsetOutput;
use crate::connector::mysql_source::output::row_data::MySQLRowDataOutput;
use crate::connector::output::{ConnectorOutput, OutputEncoding};
use crate::connector::row_data_operation::Operation;
use crate::error::{DMSRError, DMSRResult};
use crate::kafka::kafka::Kafka;
use log::debug;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Row};
use serde_json::{json, Value};
use std::collections::HashMap;

impl MySQLSourceConnector {
    pub(crate) async fn get_latest_binlog_info(&self, kafka: &Kafka) -> DMSRResult<(String, u64)> {
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
            let binlog_info: MySQLOffsetOutput = serde_json::from_value(binlog_info)?;
            return Ok((binlog_info.file().to_string(), binlog_info.pos()));
        }

        return Ok((String::new(), 0));
    }

    pub(crate) async fn lock_tables(&self, conn: &mut Conn) -> DMSRResult<()> {
        conn.query_drop("FLUSH TABLES WITH READ LOCK").await?;
        conn.query_drop("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .await?;
        conn.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
            .await?;
        Ok(())
    }

    pub(crate) async fn unlock_tables(&self, conn: &mut Conn) -> DMSRResult<()> {
        conn.query_drop("UNLOCK TABLES").await?;
        Ok(())
    }

    pub(crate) async fn finish_snapshot(&self, mut conn: Conn) -> DMSRResult<()> {
        conn.query_drop("COMMIT").await?;
        conn.disconnect().await?;
        Ok(())
    }

    pub(crate) async fn read_binlog(&self, conn: &mut Conn) -> DMSRResult<(String, u64)> {
        let query = "SHOW MASTER STATUS";
        let master_status: Vec<(String, u64, String, String, String)> = conn.query(query).await?;
        let binlog_name = &master_status[0].0;
        let binlog_name = binlog_name.to_string();
        let binlog_pos = master_status[0].1;

        Ok((binlog_name, binlog_pos))
    }

    pub(crate) async fn read_schemas(&self, conn: &mut Conn) -> DMSRResult<Vec<String>> {
        let query = r#"
            SELECT schema_name FROM information_schema.schemata
            WHERE schema_name
            NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
        "#;

        let schemas: Vec<String> = conn.query(query).await?;

        Ok(schemas)
    }

    pub(crate) async fn read_tables(&self, conn: &mut Conn) -> DMSRResult<Vec<(String, String)>> {
        let query = r#"
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema
            NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
        "#;

        let tables: Vec<(String, String)> = conn.query(query).await?;

        Ok(tables)
    }

    pub(crate) async fn read_columns(
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

    pub(crate) async fn read_ddls(
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

            let output = MySQLDDLOutput::new(ddl.1, ts_now, metadata);
            let output = ConnectorOutput::MySQLDDL(output);
            let kafka_message = output.to_kafka_message(&kafka.config, OutputEncoding::Default)?;
            kafka.produce(kafka_message).await?;
        }

        Ok(())
    }

    pub(crate) async fn read_table_data(
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

                let output = MySQLRowDataOutput::new(
                    None,
                    Some(row_data),
                    Operation::Snapshot,
                    ts_now,
                    metadata,
                );

                let output = ConnectorOutput::MySQLRowData(output);

                let kafka_message =
                    output.to_kafka_message(&kafka.config, OutputEncoding::Default)?;

                kafka.produce(kafka_message).await?;
            }
        }

        Ok(())
    }

    pub(crate) async fn update_offsets(
        &self,
        kafka: &Kafka,
        binlog_name: &str,
        binlog_pos: &u64,
    ) -> DMSRResult<()> {
        let output = MySQLOffsetOutput::new(
            self.connector_name.clone(),
            self.config.db.clone(),
            self.config.server_id,
            binlog_name.to_string(),
            *binlog_pos,
        );

        let output = ConnectorOutput::MySQLOffset(output);
        let kafka_message = output.to_kafka_message(&kafka.config, OutputEncoding::Default)?;

        kafka.produce(kafka_message).await?;

        Ok(())
    }
}
