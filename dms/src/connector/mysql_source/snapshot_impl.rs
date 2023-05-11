use crate::connector::mysql_source::connector::MySQLSourceConnector;
use crate::connector::mysql_source::metadata::source::MySQLSourceMetadata;
use crate::connector::mysql_source::output::ddl::MySQLDDLOutput;
use crate::connector::mysql_source::output::offset::MySQLOffsetOutput;
use crate::connector::mysql_source::output::row_data::MySQLRowDataOutput;
use crate::connector::output::{ConnectorOutput, OutputEncoding};
use crate::connector::row_data_operation::Operation;
use crate::error::{DMSRError, DMSRResult};
use crate::kafka::config::KafkaConfig;
use crate::kafka::kafka_client::{Kafka, RawKafkaMessageKeyValue};
use crate::kafka::message::KafkaMessage;
use async_trait::async_trait;
use log::debug;
use mysql_async::prelude::{FromRow, Queryable};
use mysql_async::{Conn, Row};
use serde_json::{json, Value};
use std::collections::HashMap;

#[async_trait]
pub(crate) trait MySQLConnSnapshot {
    async fn query_drop(&mut self, query: &str) -> DMSRResult<()>;
    async fn query<T>(&mut self, query: &str) -> DMSRResult<Vec<T>>
    where
        T: FromRow + Send + 'static;
    async fn disconnect(mut self) -> DMSRResult<()>;
}

#[async_trait]
impl MySQLConnSnapshot for Conn {
    async fn query_drop(&mut self, query: &str) -> DMSRResult<()> {
        Queryable::query_drop(self, query).await?;
        Ok(())
    }

    async fn query<T>(&mut self, query: &str) -> DMSRResult<Vec<T>>
    where
        T: FromRow + Send + 'static,
    {
        let rows = Queryable::query(self, query).await?;
        Ok(rows)
    }

    async fn disconnect(mut self) -> DMSRResult<()> {
        self.disconnect().await?;
        Ok(())
    }
}

#[async_trait]
pub(crate) trait KafkaSnapshot {
    fn config(&self) -> &KafkaConfig;

    async fn poll_with_timeout(
        &self,
        topic: &str,
        timeout: u64,
    ) -> DMSRResult<Vec<RawKafkaMessageKeyValue>>;

    async fn produce(&self, message: KafkaMessage) -> DMSRResult<()>;
}

#[async_trait]
impl KafkaSnapshot for Kafka {
    fn config(&self) -> &KafkaConfig {
        self.config()
    }

    async fn poll_with_timeout(
        &self,
        topic: &str,
        timeout: u64,
    ) -> DMSRResult<Vec<RawKafkaMessageKeyValue>> {
        self.poll_with_timeout(topic, timeout).await
    }

    async fn produce(&self, message: KafkaMessage) -> DMSRResult<()> {
        self.produce(message).await
    }
}

impl MySQLSourceConnector {
    pub(crate) async fn get_latest_binlog_info(
        &self,
        kafka: &impl KafkaSnapshot,
    ) -> DMSRResult<(String, u64)> {
        let config = kafka.config();

        let offset_topic_messages = kafka.poll_with_timeout(&config.offset_topic, 1).await?;

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

        Ok((String::new(), 0))
    }

    pub(crate) async fn lock_tables(&self, conn: &mut impl MySQLConnSnapshot) -> DMSRResult<()> {
        conn.query_drop("FLUSH TABLES WITH READ LOCK").await?;
        conn.query_drop("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .await?;
        conn.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
            .await?;
        Ok(())
    }

    pub(crate) async fn unlock_tables(&self, conn: &mut impl MySQLConnSnapshot) -> DMSRResult<()> {
        conn.query_drop("UNLOCK TABLES").await?;
        Ok(())
    }

    pub(crate) async fn finish_snapshot(&self, mut conn: impl MySQLConnSnapshot) -> DMSRResult<()> {
        conn.query_drop("COMMIT").await?;
        conn.disconnect().await?;
        Ok(())
    }

    pub(crate) async fn read_binlog(
        &self,
        conn: &mut impl MySQLConnSnapshot,
    ) -> DMSRResult<(String, u64)> {
        let query = "SHOW MASTER STATUS";
        let master_status: Vec<(String, u64, String, String, String)> = conn.query(query).await?;
        let binlog_name = &master_status[0].0;
        let binlog_name = binlog_name.to_string();
        let binlog_pos = master_status[0].1;

        Ok((binlog_name, binlog_pos))
    }

    pub(crate) async fn read_schemas(
        &self,
        conn: &mut impl MySQLConnSnapshot,
    ) -> DMSRResult<Vec<String>> {
        let query = r#"
            SELECT schema_name FROM information_schema.schemata
            WHERE schema_name
            NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
        "#;

        let schemas: Vec<String> = conn.query(query).await?;

        Ok(schemas)
    }

    pub(crate) async fn read_tables(
        &self,
        conn: &mut impl MySQLConnSnapshot,
    ) -> DMSRResult<Vec<(String, String)>> {
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
        conn: &mut impl MySQLConnSnapshot,
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
        kafka: &impl KafkaSnapshot,
        conn: &mut impl MySQLConnSnapshot,
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

            let sql = format!("SHOW CREATE TABLE {}.{}", schema, table);
            let mut res: Vec<(String, String)> = conn.query(&sql).await?;

            let ddl = res
                .pop()
                .ok_or(DMSRError::MySQLSourceConnectorError("No DDL found".into()))?;

            let output = MySQLDDLOutput::new(ddl.1, ts_now, metadata);
            let output = ConnectorOutput::MySQLDDL(output);

            let kafka_config = kafka.config();
            let kafka_message = output.to_kafka_message(kafka_config, OutputEncoding::Default)?;
            kafka.produce(kafka_message).await?;
        }

        Ok(())
    }

    pub(crate) async fn read_table_data(
        &self,
        kafka: &impl KafkaSnapshot,
        conn: &mut impl MySQLConnSnapshot,
        columns: &HashMap<(String, String), Vec<String>>,
        binlog_name: &str,
        binlog_pos: &u64,
    ) -> DMSRResult<()> {
        debug!("Reading table data...");

        for ((schema, table), column_names) in columns.iter() {
            let query = format!("SELECT * FROM {}.{}", schema, table);
            let rows: Vec<Row> = conn.query(&query).await?;

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
                    output.to_kafka_message(kafka.config(), OutputEncoding::Default)?;

                kafka.produce(kafka_message).await?;
            }
        }

        Ok(())
    }

    pub(crate) async fn update_offsets(
        &self,
        kafka: &impl KafkaSnapshot,
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
        let kafka_message = output.to_kafka_message(kafka.config(), OutputEncoding::Default)?;

        kafka.produce(kafka_message).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::mysql_source::config::MySQLSourceConfig;
    use crate::connector::source_connector::SourceConnector;
    use crate::kafka::config::KafkaConfig;
    use async_trait::async_trait;
    use mockall::{automock, mock};

    mock! {
        Conn {}
        #[async_trait]
        impl MySQLConnSnapshot for Conn {
            async fn query_drop(&mut self, query: &str) -> DMSRResult<()>;
            async fn query<T>(&mut self, query: &str) -> DMSRResult<Vec<T>>
            where
                T: FromRow + Send + 'static;
            async fn disconnect(mut self) -> DMSRResult<()>;
        }
    }

    mock! {
        Kafka {}
        #[async_trait]
        impl KafkaSnapshot for Kafka {
            fn config(&self) -> &KafkaConfig;

            async fn poll_with_timeout(
                &self,
                topic: &str,
                timeout: u64,
            ) -> DMSRResult<Vec<RawKafkaMessageKeyValue>>;

            async fn produce(&self, message: KafkaMessage) -> DMSRResult<()>;
        }
    }

    #[tokio::test]
    async fn test_get_latest_binlog_info() {
        let connector =
            MySQLSourceConnector::new(String::from("test-connector"), MySQLSourceConfig::default())
                .await
                .unwrap();

        // Case 1: No message
        let mut kafka = MockKafka::new();
        kafka.expect_config().return_const(KafkaConfig::default());
        kafka
            .expect_poll_with_timeout()
            .returning(|_, _| Ok(vec![]));

        let result = connector.get_latest_binlog_info(&kafka).await.unwrap();
        assert_eq!(result.0, "");
        assert_eq!(result.1, 0);

        // Case 2: With messages
        let mut kafka = MockKafka::new();
        kafka.expect_config().return_const(KafkaConfig::default());
        kafka
            .expect_poll_with_timeout()
            .returning(|_, _| Ok(vec![
                RawKafkaMessageKeyValue::new(
                    r#"{"connector_name": "test-connector"}"#.to_string(),
                    r#"{"connector_name": "test-connector", "db": "mysql", "server_id": 1, "file": "binlog.file", "pos": 123}"#.to_string()
                )
            ]));

        let result = connector.get_latest_binlog_info(&kafka).await.unwrap();
        assert_eq!(result.0, "binlog.file");
        assert_eq!(result.1, 123);
    }

    #[tokio::test]
    async fn test_lock_tables() {
        let connector = MySQLSourceConnector::new(String::default(), MySQLSourceConfig::default())
            .await
            .unwrap();

        let mut conn = MockConn::new();

        conn.expect_query_drop()
            .withf(|query| query.eq("FLUSH TABLES WITH READ LOCK"))
            .times(1)
            .returning(|_| Ok(()));

        conn.expect_query_drop()
            .withf(|query| query.eq("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            .times(1)
            .returning(|_| Ok(()));

        conn.expect_query_drop()
            .withf(|query| query.eq("START TRANSACTION WITH CONSISTENT SNAPSHOT"))
            .times(1)
            .returning(|_| Ok(()));

        let result = connector.lock_tables(&mut conn).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_unlock_tables() {
        let connector = MySQLSourceConnector::new(String::default(), MySQLSourceConfig::default())
            .await
            .unwrap();

        let mut conn = MockConn::new();
        conn.expect_query_drop()
            .withf(|query| query.contains("UNLOCK TABLES"))
            .times(1)
            .returning(|_| Ok(()));

        let result = connector.unlock_tables(&mut conn).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_finish_snapshot() {
        let connector = MySQLSourceConnector::new(String::default(), MySQLSourceConfig::default())
            .await
            .unwrap();

        let mut conn = MockConn::new();
        conn.expect_query_drop()
            .withf(|query| query.contains("COMMIT"))
            .times(1)
            .returning(|_| Ok(()));

        conn.expect_disconnect().times(1).returning(|| Ok(()));

        let result = connector.finish_snapshot(conn).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_read_binlog() {
        let connector = MySQLSourceConnector::new(String::default(), MySQLSourceConfig::default())
            .await
            .unwrap();

        let mut conn = MockConn::new();

        conn.expect_query()
            .withf(|query| query.eq("SHOW MASTER STATUS"))
            .times(1)
            .returning(|_| {
                Ok(vec![(
                    "binlog.file".to_string(),
                    123_u64,
                    "".to_string(),
                    "".to_string(),
                    "".to_string(),
                )])
            });

        let result = connector.read_binlog(&mut conn).await.unwrap();
        assert_eq!(result.0, "binlog.file");
        assert_eq!(result.1, 123);
    }

    #[tokio::test]
    async fn test_read_schemas() {
        let connector = MySQLSourceConnector::new(String::default(), MySQLSourceConfig::default())
            .await
            .unwrap();

        let mut conn = MockConn::new();

        let query = r#"
            SELECT schema_name FROM information_schema.schemata
            WHERE schema_name
            NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
        "#;
        conn.expect_query()
            .withf(|q| q.eq(query))
            .times(1)
            .returning(|_| Ok(vec!["db1".to_string(), "db2".to_string()]));

        let result = connector.read_schemas(&mut conn).await.unwrap();
        assert_eq!(result, vec!["db1", "db2"]);
    }

    #[tokio::test]
    async fn test_read_tables() {
        let connector = MySQLSourceConnector::new(String::default(), MySQLSourceConfig::default())
            .await
            .unwrap();

        let mut conn = MockConn::new();

        let query = r#"
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema
            NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
        "#;

        conn.expect_query()
            .withf(|q| q.eq(query))
            .times(1)
            .returning(move |_| {
                Ok(vec![
                    ("test_schema".to_string(), "tbl_1".to_string()),
                    ("test_schema".to_string(), "tbl_2".to_string()),
                ])
            });

        let result = connector.read_tables(&mut conn).await.unwrap();
        assert_eq!(
            result,
            vec![
                ("test_schema".to_string(), "tbl_1".to_string()),
                ("test_schema".to_string(), "tbl_2".to_string()),
            ]
        );
    }

    #[tokio::test]
    async fn test_read_columns() {
        let connector = MySQLSourceConnector::new(String::default(), MySQLSourceConfig::default())
            .await
            .unwrap();

        let mut conn = MockConn::new();

        let query = r#"
            SELECT table_schema, table_name, column_name
            FROM information_schema.columns
            WHERE table_schema
            NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
        "#;

        conn.expect_query()
            .withf(|q| q.eq(query))
            .times(1)
            .returning(move |_| {
                Ok(vec![
                    (
                        "test_schema".to_string(),
                        "tbl_1".to_string(),
                        "col_1".to_string(),
                    ),
                    (
                        "test_schema".to_string(),
                        "tbl_1".to_string(),
                        "col_2".to_string(),
                    ),
                    (
                        "test_schema".to_string(),
                        "tbl_2".to_string(),
                        "col_1".to_string(),
                    ),
                ])
            });

        let result = connector.read_columns(&mut conn).await.unwrap();

        let tbl_1 = ("test_schema".to_string(), "tbl_1".to_string());
        assert_eq!(result.get(&tbl_1).unwrap(), &vec!["col_1", "col_2"]);

        let tbl_2 = ("test_schema".to_string(), "tbl_2".to_string());
        assert_eq!(result.get(&tbl_2).unwrap(), &vec!["col_1"]);
    }

    #[tokio::test]
    async fn test_read_ddls() {
        let connector = MySQLSourceConnector::new(String::default(), MySQLSourceConfig::default())
            .await
            .unwrap();

        let mut conn = MockConn::new();


    }
}
