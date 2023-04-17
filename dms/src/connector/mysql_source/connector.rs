use crate::connector::connector::Connector;
use crate::connector::mysql_source::config::MySQLSourceConfig;
use crate::connector::mysql_source::decoding::{
    parse_table_name_from_sqlparser_object_name, parse_table_name_from_table_map_event, MySQLTable,
    MySQLTableColumn,
};
use crate::error::error::{DMSRError, DMSRResult};
use crate::kafka::kafka::Kafka;
use crate::message::message::{KafkaMessage, Operation, Payload};
use crate::message::mysql_source::MySQLSource;
use async_trait::async_trait;
use futures::StreamExt;
use log::debug;
use mysql_async::binlog::events::{
    DeleteRowsEvent, Event, QueryEvent, RotateEvent, RowsEventRows, TableMapEvent, UpdateRowsEvent,
    WriteRowsEvent,
};
use mysql_async::binlog::row::BinlogRow;
use mysql_async::binlog::value::BinlogValue;
use mysql_async::binlog::EventType;
use mysql_async::{BinlogRequest, BinlogStream, Pool, Value};
use serde_json::json;
use sqlparser::ast::{
    AlterColumnOperation, AlterTableOperation, ColumnDef, ColumnOption, Ident, ObjectName,
    Statement, TableConstraint,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;

pub struct MySQLSourceConnector<'a> {
    config: MySQLSourceConfig,
    connector_name: String,
    cdc_stream: BinlogStream,
    table_def_map: HashMap<String, MySQLTable>,
    last_table_map_event: Option<TableMapEvent<'a>>,
    binlog_file_name: String,
}

#[async_trait]
impl<'a> Connector for MySQLSourceConnector<'a> {
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
            table_def_map: HashMap::new(),
            last_table_map_event: None,
            binlog_file_name: "".into(),
        }))
    }

    async fn stream(&mut self, kafka: &Kafka) -> DMSRResult<()> {
        while let Some(event) = self.cdc_stream.next().await {
            let event = event?;

            let ts = 1000 * (event.fde().create_timestamp() as u64);
            let header = event.header();
            let log_pos = header.log_pos() as u64;

            let event_type = header.event_type()?;

            debug!("**Event_type: {:?}**", event_type);

            match event_type {
                EventType::QUERY_EVENT => self.handle_query_event(event).await?,
                EventType::TABLE_MAP_EVENT => self.handle_table_map_event(event)?,
                EventType::WRITE_ROWS_EVENT => {
                    let kafka_messages =
                        self.handle_rows_event(event, EventType::WRITE_ROWS_EVENT, ts, log_pos);
                    debug!("WRITE_ROWS_EVENT - Kafka Messages: {:?}", kafka_messages);
                }
                EventType::UPDATE_ROWS_EVENT => {
                    let kafka_messages =
                        self.handle_rows_event(event, EventType::UPDATE_ROWS_EVENT, ts, log_pos);
                    debug!("UPDATE_ROWS_EVENT - Kafka Messages: {:?}", kafka_messages);
                }
                EventType::DELETE_ROWS_EVENT => {
                    let kafka_messages =
                        self.handle_rows_event(event, EventType::DELETE_ROWS_EVENT, ts, log_pos);
                    debug!("DELETE_ROWS_EVENT - Kafka Messages: {:?}", kafka_messages);
                }
                EventType::ROTATE_EVENT => self.handle_rotate_event(event)?,
                _ => {}
            }
        }
        Ok(())
    }
}

impl<'a> MySQLSourceConnector<'a> {
    async fn update_table_def(&mut self, schema: String, query: String) -> DMSRResult<()> {
        debug!("update_table_def - Query: {}", query);

        let dialect = MySqlDialect {};
        let ast: Vec<Statement> = Parser::parse_sql(&dialect, query.as_str())?;

        match ast.first() {
            Some(Statement::CreateTable {
                name,
                columns,
                constraints,
                ..
            }) => self.handle_create_table(&schema, name, columns, constraints)?,

            Some(Statement::AlterTable { name, operation }) => {
                self.handle_alter_table(&schema, name, operation)?
            }
            _ => {}
        }

        Ok(())
    }

    fn handle_create_table(
        &mut self,
        schema: &str,
        name: &ObjectName,
        columns: &[ColumnDef],
        constraints: &[TableConstraint],
    ) -> DMSRResult<()> {
        let (schema, table_name, full_table_name) =
            parse_table_name_from_sqlparser_object_name(schema, name)?;
        let mut mysql_table_columns: Vec<MySQLTableColumn> = vec![];

        for c in columns {
            let column_name = c.name.to_string();
            let data_type = c.data_type.to_string();

            let is_nullable = !c
                .options
                .iter()
                .map(|def| &def.option)
                .any(|o| matches!(o, ColumnOption::NotNull));

            let is_primary_key = c.options.iter().map(|def| &def.option).any(|o| match o {
                ColumnOption::Unique { is_primary } => *is_primary,
                _ => false,
            });

            mysql_table_columns.push(MySQLTableColumn {
                column_name,
                data_type,
                is_nullable: is_nullable && !is_primary_key,
                is_primary_key,
            });
        }

        for c in constraints {
            if let TableConstraint::Unique {
                columns,
                is_primary,
                ..
            } = c
            {
                if !is_primary {
                    continue;
                }
                for col in columns {
                    let col = col.to_string();
                    let col = mysql_table_columns
                        .iter_mut()
                        .find(|c| c.column_name == col);
                    if col.is_none() {
                        continue;
                    }
                    let col = col.unwrap();
                    col.is_nullable = false;
                    col.is_primary_key = true;
                }
            }
        }

        let mysql_table = MySQLTable {
            schema_name: schema,
            table_name,
            columns: mysql_table_columns,
        };
        self.table_def_map.insert(full_table_name, mysql_table);
        Ok(())
    }

    fn handle_alter_table(
        &mut self,
        schema: &str,
        name: &ObjectName,
        operation: &AlterTableOperation,
    ) -> DMSRResult<()> {
        let (schema, _, full_table_name) =
            parse_table_name_from_sqlparser_object_name(schema, name)?;

        match operation {
            AlterTableOperation::AddColumn { column_def, .. } => {
                self.handle_add_column(column_def, &full_table_name)?
            }
            AlterTableOperation::AlterColumn { column_name, op } => {
                self.handle_alter_column(column_name, op, &full_table_name)?
            }
            AlterTableOperation::DropColumn { column_name, .. } => {
                self.handle_drop_column(column_name, &full_table_name)?
            }
            AlterTableOperation::DropPrimaryKey => {
                self.handle_drop_primary_key(&full_table_name)?
            }
            AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => self.handle_rename_column(old_column_name, new_column_name, &full_table_name)?,
            AlterTableOperation::RenameTable { table_name } => {
                self.handle_rename_table(table_name, &full_table_name, &schema)?
            }
            _ => {}
        }

        Ok(())
    }

    fn handle_add_column(
        &mut self,
        column_def: &ColumnDef,
        full_table_name: &str,
    ) -> DMSRResult<()> {
        let column_name = column_def.name.to_string();
        let data_type = column_def.data_type.to_string();
        let is_nullable = !column_def
            .options
            .iter()
            .map(|def| &def.option)
            .any(|o| matches!(o, ColumnOption::NotNull));

        let is_primary_key = column_def
            .options
            .iter()
            .map(|def| &def.option)
            .any(|o| match o {
                ColumnOption::Unique { is_primary } => *is_primary,
                _ => false,
            });

        let table = self.retrieve_table_meta_mut(full_table_name)?;
        table.columns.push(MySQLTableColumn {
            column_name,
            data_type,
            is_nullable: is_nullable && !is_primary_key,
            is_primary_key,
        });

        Ok(())
    }

    fn handle_alter_column(
        &mut self,
        column_name: &Ident,
        op: &AlterColumnOperation,
        full_table_name: &str,
    ) -> DMSRResult<()> {
        let column_name = column_name.to_string();
        let col = self
            .retrieve_table_meta_mut(full_table_name)?
            .get_column_mut(&column_name)?;

        match op {
            AlterColumnOperation::SetDataType { data_type, .. } => {
                col.data_type = data_type.to_string();
            }
            AlterColumnOperation::SetNotNull => {
                col.is_nullable = false;
            }
            AlterColumnOperation::DropNotNull => {
                col.is_nullable = true;
            }
            _ => {}
        }
        Ok(())
    }

    fn handle_drop_column(&mut self, column_name: &Ident, full_table_name: &str) -> DMSRResult<()> {
        let column_name = column_name.to_string();
        let table = self.retrieve_table_meta_mut(full_table_name)?;
        table.columns = table
            .columns
            .clone()
            .into_iter()
            .filter(|c| c.column_name != column_name)
            .collect();
        Ok(())
    }

    fn handle_drop_primary_key(&mut self, full_table_name: &str) -> DMSRResult<()> {
        let table = self.retrieve_table_meta_mut(full_table_name)?;
        table.columns = table
            .columns
            .clone()
            .into_iter()
            .map(|mut c| {
                c.is_primary_key = false;
                c
            })
            .collect();
        Ok(())
    }

    fn handle_rename_column(
        &mut self,
        old_column_name: &Ident,
        new_column_name: &Ident,
        full_table_name: &str,
    ) -> DMSRResult<()> {
        let old_column_name = old_column_name.to_string();
        let new_column_name = new_column_name.to_string();
        let table = self.retrieve_table_meta_mut(full_table_name)?;
        let col = table.get_column_mut(&old_column_name)?;
        col.column_name = new_column_name;
        Ok(())
    }

    fn handle_rename_table(
        &mut self,
        table_name: &ObjectName,
        full_table_name: &str,
        schema: &str,
    ) -> DMSRResult<()> {
        let (_, _, new_full_table_name) =
            parse_table_name_from_sqlparser_object_name(schema, table_name)?;
        let mut table = self.retrieve_table_meta(full_table_name)?.clone();
        table.table_name = table_name.to_string();
        self.table_def_map.insert(new_full_table_name, table);
        self.table_def_map.remove(full_table_name);
        Ok(())
    }

    async fn handle_query_event(&mut self, event: Event) -> DMSRResult<()> {
        let event = event.read_event::<QueryEvent>()?;
        debug!("QUERY_EVENT: {:?}\n", event);
        let schema = event.schema();
        let query = event.query();
        self.update_table_def(schema.into(), query.into()).await?;
        Ok(())
    }

    fn handle_table_map_event(&mut self, event: Event) -> DMSRResult<()> {
        let event = event.read_event::<TableMapEvent>()?;
        debug!("TABLE_MAP_EVENT: {:?}\n", event);
        self.last_table_map_event = Some(event.into_owned());
        Ok(())
    }

    fn handle_rotate_event(&mut self, event: Event) -> DMSRResult<()> {
        let event = event.read_event::<RotateEvent>()?;
        debug!("ROTATE_EVENT: {:?}\n", event);
        self.binlog_file_name = event.name().to_string();
        Ok(())
    }

    fn handle_rows_event(
        &mut self,
        event: Event,
        event_type: EventType,
        ts: u64,
        log_pos: u64,
    ) -> DMSRResult<Vec<KafkaMessage<MySQLSource>>> {
        let table_event = self.retrieve_last_table_event()?;

        let (schema, table_name, full_table_name) =
            parse_table_name_from_table_map_event(table_event)?;

        let table_metadata = self.retrieve_table_meta(&full_table_name)?;
        let table_columns = &table_metadata.columns;
        let num_columns = table_columns.len();

        let kafka_messages: Vec<KafkaMessage<MySQLSource>>;

        match event_type {
            EventType::WRITE_ROWS_EVENT => {
                let event = event.read_event::<WriteRowsEvent>()?;
                debug!("WRITE_ROWS_EVENT: {:?}\n", event);
                let kafka_messages: Vec<KafkaMessage<MySQLSource>> = event
                    .rows(table_event)
                    .into_iter()
                    .filter_map(|r| r.ok())
                    .map(|r| {
                        (
                            self.parse_binlog_rows(r.0, num_columns, table_columns),
                            self.parse_binlog_rows(r.1, num_columns, table_columns),
                        )
                    })
                    .filter_map(|payload| {
                        self.construct_kafka_message(
                            payload.0,
                            payload.1,
                            table_metadata,
                            ts,
                            log_pos,
                        )
                        .ok()
                    })
                    .collect();
            }
            EventType::UPDATE_ROWS_EVENT => {
                let event = event.read_event::<UpdateRowsEvent>()?;
                debug!("UPDATE_ROWS_EVENT: {:?}\n", event);
                let kafka_messages: Vec<KafkaMessage<MySQLSource>> = event
                    .rows(table_event)
                    .into_iter()
                    .filter_map(|r| r.ok())
                    .map(|r| {
                        (
                            self.parse_binlog_rows(r.0, num_columns, table_columns),
                            self.parse_binlog_rows(r.1, num_columns, table_columns),
                        )
                    })
                    .filter_map(|payload| {
                        self.construct_kafka_message(
                            payload.0,
                            payload.1,
                            table_metadata,
                            ts,
                            log_pos,
                        )
                        .ok()
                    })
                    .collect();
            }
            EventType::DELETE_ROWS_EVENT => {
                let event = event.read_event::<DeleteRowsEvent>()?;
                debug!("DELETE_ROWS_EVENT: {:?}\n", event);
                let kafka_messages: Vec<KafkaMessage<MySQLSource>> = event
                    .rows(table_event)
                    .into_iter()
                    .filter_map(|r| r.ok())
                    .map(|r| {
                        (
                            self.parse_binlog_rows(r.0, num_columns, table_columns),
                            self.parse_binlog_rows(r.1, num_columns, table_columns),
                        )
                    })
                    .filter_map(|payload| {
                        self.construct_kafka_message(
                            payload.0,
                            payload.1,
                            table_metadata,
                            ts,
                            log_pos,
                        )
                        .ok()
                    })
                    .collect();
            }
            _ => {
                return Err(DMSRError::MySQLSourceConnectorError(
                    "unsupported event type".into(),
                ))
            }
        }

        Ok(vec![])
    }

    fn parse_binlog_rows(
        &self,
        row: Option<BinlogRow>,
        num_columns: usize,
        table_columns: &[MySQLTableColumn],
    ) -> serde_json::Value {
        if row.is_none() {
            return serde_json::Value::Null;
        }

        let row = row.unwrap();

        let mut payload = json!({});
        for n in 0..num_columns {
            let value = row.as_ref(n);
            let col = table_columns.get(n);

            if value.is_none() || col.is_none() {
                return serde_json::Value::Null;
            }

            let value = value.unwrap();
            let col = col.unwrap();

            match value {
                BinlogValue::Value(Value::NULL) => {
                    payload[col.column_name.as_str()] = json!(null);
                }
                BinlogValue::Value(v) => {
                    let v = v.as_sql(false);
                    payload[col.column_name.as_str()] = json!(v);
                }
                BinlogValue::Jsonb(_) => {}
                BinlogValue::JsonDiff(_) => {}
            }
        }

        payload
    }

    fn construct_kafka_message(
        &self,
        before: serde_json::Value,
        after: serde_json::Value,
        table_metadata: &MySQLTable,
        ts: u64,
        log_pos: u64,
    ) -> DMSRResult<KafkaMessage<MySQLSource>> {
        let full_table_name = format!(
            "{}.{}",
            table_metadata.schema_name, table_metadata.table_name
        );
        let kafka_message_schema = table_metadata.as_kafka_message_schema(&full_table_name)?;

        let kafka_message_source = MySQLSource::new(
            self.connector_name.clone(),
            table_metadata.schema_name.clone(),
            table_metadata.table_name.clone(),
            self.config.server_id,
            self.binlog_file_name.clone(),
            log_pos,
        );

        let kafka_message_payload: Payload<MySQLSource> = Payload::new(
            Some(before),
            Some(after),
            Operation::Create,
            ts,
            kafka_message_source,
        );

        let kafka_message = KafkaMessage::new(kafka_message_schema, kafka_message_payload);
        debug!(
            "kafka_message: {:?}\n",
            serde_json::to_string(&kafka_message)
        );
        Ok(kafka_message)
    }

    fn retrieve_last_table_event(&self) -> DMSRResult<&TableMapEvent> {
        self.last_table_map_event
            .as_ref()
            .ok_or(DMSRError::MySQLSourceConnectorError(
                "No table map event found".into(),
            ))
    }

    fn retrieve_table_meta_mut(&mut self, table_name: &str) -> DMSRResult<&mut MySQLTable> {
        self.table_def_map
            .get_mut(table_name)
            .ok_or(DMSRError::MySQLSourceConnectorError(
                "No table metadata found".into(),
            ))
    }

    fn retrieve_table_meta(&self, table_name: &str) -> DMSRResult<&MySQLTable> {
        self.table_def_map
            .get(table_name)
            .ok_or(DMSRError::MySQLSourceConnectorError(
                "No table metadata found".into(),
            ))
    }
}
