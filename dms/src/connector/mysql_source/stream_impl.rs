use crate::connector::mysql_source::binlog_struct::{MySQLTable, MySQLTableColumn};
use crate::connector::mysql_source::connector::MySQLSourceConnector;
use crate::connector::mysql_source::metadata::source::MySQLSourceMetadata;
use crate::connector::mysql_source::output::ddl::MySQLDDLOutput;
use crate::connector::mysql_source::output::row_data::MySQLRowDataOutput;
use crate::connector::output::ConnectorOutput;
use crate::connector::row_data_operation::Operation;
use crate::error::{DMSRError, DMSRResult};
use crate::kafka::kafka_client::{Kafka, RawKafkaMessageKeyValue};
use async_trait::async_trait;
use log::{debug, error};
use mysql_async::binlog::events::{
    DeleteRowsEvent, Event, QueryEvent, RotateEvent, RowsEventRows, TableMapEvent, UpdateRowsEvent,
    WriteRowsEvent,
};
use mysql_async::binlog::row::BinlogRow;
use mysql_async::binlog::value::BinlogValue;
use mysql_async::binlog::EventType;
use mysql_async::{BinlogRequest, BinlogStream, Conn, Value};
use mysql_common::binlog::events::{BinlogEventHeader, FormatDescriptionEvent};
use mysql_common::binlog::BinlogEvent;
use serde_json::json;
use sqlparser::ast::{
    AlterColumnOperation, AlterTableOperation, ColumnDef, ColumnOption, Ident, ObjectName,
    ObjectType, Statement, TableConstraint,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use std::io::Error;

enum RowsEvent<'a> {
    Write(WriteRowsEvent<'a>),
    Update(UpdateRowsEvent<'a>),
    Delete(DeleteRowsEvent<'a>),
}

#[async_trait]
pub(crate) trait KafkaInterface {
    async fn poll_with_timeout(
        &self,
        topic: &str,
        timeout: u64,
    ) -> DMSRResult<Vec<RawKafkaMessageKeyValue>>;
}

#[async_trait]
impl KafkaInterface for Kafka {
    async fn poll_with_timeout(
        &self,
        topic: &str,
        timeout: u64,
    ) -> DMSRResult<Vec<RawKafkaMessageKeyValue>> {
        self.poll_with_timeout(topic, timeout).await
    }
}

#[async_trait]
pub(crate) trait MySQLInterface {
    async fn get_binlog_stream(
        mut self,
        server_id: u32,
        binlog_pos: Option<u64>,
        binlog_name: Option<&String>,
    ) -> DMSRResult<BinlogStream>;
}

#[async_trait]
impl MySQLInterface for Conn {
    async fn get_binlog_stream(
        mut self,
        server_id: u32,
        binlog_pos: Option<u64>,
        binlog_name: Option<&String>,
    ) -> DMSRResult<BinlogStream> {
        let mut binlog_request = BinlogRequest::new(server_id);

        if let (Some(binlog_pos), Some(binlog_name)) = (binlog_pos, binlog_name) {
            let binlog_name = binlog_name.as_bytes();
            binlog_request = binlog_request
                .with_filename(binlog_name)
                .with_pos(binlog_pos);
        }

        let binlog_stream = self.get_binlog_stream(binlog_request).await?;
        Ok(binlog_stream)
    }
}

pub(crate) trait EventInterface {
    fn fde(&self) -> &FormatDescriptionEvent;
    fn header(&self) -> BinlogEventHeader;
    fn read_event<'a, 'b: 'a, T: BinlogEvent<'a>>(&'b self) -> Result<T, Error>;
}

impl EventInterface for Event {
    fn fde(&self) -> &FormatDescriptionEvent {
        self.fde()
    }

    fn header(&self) -> BinlogEventHeader {
        self.header()
    }

    fn read_event<'a, 'b: 'a, T: BinlogEvent<'a>>(&'b self) -> Result<T, Error> {
        let event = self.read_event::<T>()?;
        Ok(event)
    }
}

pub(crate) trait QueryEventInterface<'a> {
    fn schema(&self) -> String;
    fn query(&self) -> String;
}

impl<'a> QueryEventInterface<'a> for QueryEvent<'a> {
    fn schema(&self) -> String {
        self.schema().to_string()
    }

    fn query(&self) -> String {
        self.query().to_string()
    }
}

impl MySQLSourceConnector {
    pub(crate) async fn get_binlog_stream(
        &self,
        conn: impl MySQLInterface,
    ) -> DMSRResult<BinlogStream> {
        let cdc_stream = conn
            .get_binlog_stream(
                self.config.server_id,
                self.binlog_pos,
                self.binlog_file_name.as_ref(),
            )
            .await?;
        Ok(cdc_stream)
    }

    pub(crate) async fn read_historical_schema_changes(
        &mut self,
        kafka: &impl KafkaInterface,
    ) -> DMSRResult<()> {
        let schema_messages = kafka.poll_with_timeout(&self.connector_name, 1).await?;
        for msg in schema_messages {
            let key_json = serde_json::from_str::<serde_json::Value>(&msg.key)?;
            let schema = key_json
                .get("schema")
                .ok_or(DMSRError::MySQLSourceConnectorError(
                    "Schema not found in schema message".into(),
                ))?
                .as_str()
                .unwrap_or_default();

            let value = serde_json::from_str::<MySQLDDLOutput>(&msg.value)?;
            self.parse_ddl_query(schema, &value.ddl, value.ts_ms, value.metadata.pos)?;
        }
        Ok(())
    }

    pub(crate) fn parse(&mut self, event: impl EventInterface) -> DMSRResult<Vec<ConnectorOutput>> {
        let ts = 1000 * (event.fde().create_timestamp() as u64);
        let log_pos = event.header().log_pos() as u64;
        let event_type = event.header().event_type()?;

        let mut payloads: Vec<ConnectorOutput> = vec![];
        match event_type {
            EventType::QUERY_EVENT => {
                let event = event.read_event::<QueryEvent>()?;
                if let Ok(message) = self.parse_query_event(event, ts, log_pos) {
                    payloads = message;
                }
            }
            EventType::TABLE_MAP_EVENT => {
                let event = event.read_event::<TableMapEvent>()?;
                let result = self.parse_table_map_event(event);
                if let Err(e) = result {
                    error!("Error parsing table map event: {:?}", e);
                }
            }
            EventType::ROTATE_EVENT => {
                let event = &event.read_event::<RotateEvent>()?;
                let result = self.parse_rotate_event(event);
                if let Err(e) = result {
                    error!("Error parsing rotate event: {:?}", e);
                }
            }
            EventType::WRITE_ROWS_EVENT => {
                let event = &event.read_event::<WriteRowsEvent>()?;
                let event = RowsEvent::Write(event.clone());

                match self.parse_rows_event(&event, ts, log_pos) {
                    Ok(messages) => {
                        payloads = messages;
                    }
                    Err(e) => {
                        error!("Error parsing write rows event: {:?}", e);
                    }
                }
            }
            EventType::UPDATE_ROWS_EVENT => {
                let event = &event.read_event::<UpdateRowsEvent>()?;
                let event = RowsEvent::Update(event.clone());

                match self.parse_rows_event(&event, ts, log_pos) {
                    Ok(messages) => {
                        return Ok(messages);
                    }
                    Err(e) => {
                        error!("Error parsing update rows event: {:?}", e);
                    }
                }
            }
            EventType::DELETE_ROWS_EVENT => {
                let event = &event.read_event::<DeleteRowsEvent>()?;
                let event = RowsEvent::Delete(event.clone());

                match self.parse_rows_event(&event, ts, log_pos) {
                    Ok(messages) => {
                        return Ok(messages);
                    }
                    Err(e) => {
                        error!("Error parsing delete rows event: {:?}", e);
                    }
                }
            }
            _ => {
                debug!("Event type not supported: {:?}", event_type);
            }
        }

        Ok(payloads)
    }

    fn parse_rows_event(
        &mut self,
        event: &RowsEvent,
        ts: u64,
        log_pos: u64,
    ) -> DMSRResult<Vec<ConnectorOutput>> {
        let last_table_map_event = self.last_table_map_event_as_ref()?;

        let (schema, table_name) =
            Self::parse_schema_table_from_table_map_event(last_table_map_event)?;

        let full_table_name = format!("{}.{}", schema, table_name);

        let table_metadata = self.table_meta_as_ref(&full_table_name)?;
        let table_columns = &table_metadata.columns;
        let num_columns = table_columns.len();

        let payloads = match event {
            RowsEvent::Write(e) => {
                let rows = e.rows(last_table_map_event);
                let rows_data = self.parse_rows_in_rows_event(rows, num_columns, table_columns);
                self.rows_data_into_kafka_message(
                    rows_data,
                    table_metadata,
                    ts,
                    log_pos,
                    Operation::Create,
                )?
            }
            RowsEvent::Update(e) => {
                let rows = e.rows(last_table_map_event);
                let rows_data = self.parse_rows_in_rows_event(rows, num_columns, table_columns);
                self.rows_data_into_kafka_message(
                    rows_data,
                    table_metadata,
                    ts,
                    log_pos,
                    Operation::Update,
                )?
            }
            RowsEvent::Delete(e) => {
                let rows = e.rows(last_table_map_event);
                let rows_data = self.parse_rows_in_rows_event(rows, num_columns, table_columns);
                self.rows_data_into_kafka_message(
                    rows_data,
                    table_metadata,
                    ts,
                    log_pos,
                    Operation::Delete,
                )?
            }
        };

        Ok(payloads)
    }

    fn parse_rows_in_rows_event(
        &self,
        rows: RowsEventRows,
        num_columns: usize,
        table_columns: &[MySQLTableColumn],
    ) -> Vec<(serde_json::Value, serde_json::Value)> {
        rows.into_iter()
            .filter_map(|r| r.ok())
            .map(|r| {
                (
                    Self::parse_binlog_rows(r.0, num_columns, table_columns),
                    Self::parse_binlog_rows(r.1, num_columns, table_columns),
                )
            })
            .filter_map(|r| {
                if r.0.is_ok() && r.1.is_ok() {
                    Some((r.0.unwrap(), r.1.unwrap()))
                } else {
                    None
                }
            })
            .collect()
    }

    fn parse_binlog_rows(
        row: Option<BinlogRow>,
        num_columns: usize,
        table_columns: &[MySQLTableColumn],
    ) -> DMSRResult<serde_json::Value> {
        if row.is_none() {
            return Ok(serde_json::Value::Null);
        }
        let row = row.unwrap();
        let mut payload = json!({});
        for n in 0..num_columns {
            let value = row.as_ref(n);
            let col = table_columns.get(n);

            if value.is_none() || col.is_none() {
                return Ok(serde_json::Value::Null);
            }

            let value = value.unwrap();
            let col = col.unwrap();
            let col_name = col.column_name.as_str();

            match value {
                BinlogValue::Value(Value::NULL) => {
                    payload[col_name] = json!(null);
                }
                BinlogValue::Value(Value::Bytes(v)) => {
                    let v = String::from_utf8(v.to_vec())?;
                    payload[col_name] = json!(v);
                }
                BinlogValue::Value(Value::Int(v)) => {
                    payload[col_name] = json!(v);
                }
                BinlogValue::Value(Value::UInt(v)) => {
                    payload[col_name] = json!(v);
                }
                BinlogValue::Value(Value::Float(v)) => {
                    payload[col_name] = json!(v);
                }
                BinlogValue::Value(Value::Double(v)) => {
                    payload[col_name] = json!(v);
                }
                BinlogValue::Value(v) => {
                    let v = v.as_sql(true);
                    let v = v.trim_matches('\'');
                    payload[col_name] = json!(v);
                }
                BinlogValue::Jsonb(_) => {}
                BinlogValue::JsonDiff(_) => {}
            }
        }

        Ok(payload)
    }

    fn rows_data_into_kafka_message(
        &self,
        rows_data: Vec<(serde_json::Value, serde_json::Value)>,
        table_metadata: &MySQLTable,
        ts: u64,
        log_pos: u64,
        op: Operation,
    ) -> DMSRResult<Vec<ConnectorOutput>> {
        let mut outputs: Vec<ConnectorOutput> = vec![];

        for (before, after) in rows_data {
            let msg_source = MySQLSourceMetadata::new(
                &self.connector_name,
                &self.config.db,
                &table_metadata.schema_name,
                &table_metadata.table_name,
                self.config.server_id,
                self.binlog_file_name()?,
                log_pos,
            );

            let output = MySQLRowDataOutput::new(
                Some(before),
                Some(after),
                op.clone(),
                ts,
                msg_source.clone(),
            );
            let output = ConnectorOutput::MySQLRowData(output);

            outputs.push(output);
        }

        Ok(outputs)
    }

    fn parse_rotate_event(&mut self, event: &RotateEvent) -> DMSRResult<()> {
        self.binlog_file_name = Some(event.name().to_string());
        Ok(())
    }

    fn parse_table_map_event<'a>(&'a mut self, event: TableMapEvent<'a>) -> DMSRResult<()> {
        self.last_table_map_event = Some(event.into_owned());
        Ok(())
    }

    fn parse_query_event<'a>(
        &mut self,
        event: impl QueryEventInterface<'a>,
        ts: u64,
        log_pos: u64,
    ) -> DMSRResult<Vec<ConnectorOutput>> {
        let schema = event.schema();
        let query = event.query();
        self.parse_ddl_query(&schema, &query, ts, log_pos)
    }

    pub(crate) fn parse_ddl_query(
        &mut self,
        schema: &str,
        query: &str,
        ts: u64,
        log_pos: u64,
    ) -> DMSRResult<Vec<ConnectorOutput>> {
        let dialect = MySqlDialect {};
        let ast: Vec<Statement> = Parser::parse_sql(&dialect, query)?;
        let stmt = ast.first().ok_or(DMSRError::MySQLSourceConnectorError(
            format!("Query not supported by parser: {}", query).into(),
        ))?;

        let payloads = match stmt {
            Statement::CreateTable {
                name,
                columns,
                constraints,
                ..
            } => {
                let (schema, table) = Self::parse_schema_table_from_sqlparser(schema, name)?;
                self.parse_create_table_statement(&schema, &table, columns, constraints)?;
                let msg = self.query_event_to_kafka_message(&schema, &table, query, ts, log_pos)?;
                Ok(vec![msg])
            }
            Statement::AlterTable {
                name, operation, ..
            } => {
                let (schema, table) = Self::parse_schema_table_from_sqlparser(schema, name)?;
                self.parse_alter_table_statement(&schema, &table, operation)?;
                let msg = self.query_event_to_kafka_message(&schema, &table, query, ts, log_pos)?;
                Ok(vec![msg])
            }
            Statement::Drop {
                object_type, names, ..
            } => {
                let dropped_tables = self.parse_drop_statement(schema, object_type, names)?;

                let msg: Vec<ConnectorOutput> = dropped_tables
                    .iter()
                    .filter_map(|(schema, table)| {
                        self.query_event_to_kafka_message(schema, table, query, ts, log_pos)
                            .ok()
                    })
                    .collect();

                Ok(msg)
            }
            Statement::Truncate { table_name, .. } => {
                let (schema, table) = Self::parse_schema_table_from_sqlparser(schema, table_name)?;
                let msg = self.parse_truncate_statement(&schema, &table, ts, log_pos)?;
                Ok(vec![msg])
            }
            _ => Err(DMSRError::MySQLSourceConnectorError(
                format!("Query not supported by parser: {}", query).into(),
            )),
        };

        payloads
    }

    fn query_event_to_kafka_message(
        &self,
        schema: &str,
        table: &str,
        ddl: &str,
        ts: u64,
        log_pos: u64,
    ) -> DMSRResult<ConnectorOutput> {
        let metadata = MySQLSourceMetadata::new(
            &self.connector_name,
            &self.config.db,
            schema,
            table,
            self.config.server_id,
            self.binlog_file_name()?,
            log_pos,
        );

        let output = MySQLDDLOutput::new(ddl.to_string(), ts, metadata);
        let output = ConnectorOutput::MySQLDDL(output);

        Ok(output)
    }

    fn parse_create_table_statement(
        &mut self,
        schema: &str,
        table: &str,
        columns: &[ColumnDef],
        constraints: &[TableConstraint],
    ) -> DMSRResult<()> {
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

        let full_table_name = format!("{}.{}", schema, table);

        let mysql_table = MySQLTable {
            schema_name: schema.to_string(),
            table_name: table.to_string(),
            columns: mysql_table_columns,
        };

        self.table_metadata_map.insert(full_table_name, mysql_table);

        Ok(())
    }

    fn parse_alter_table_statement(
        &mut self,
        schema: &str,
        table: &str,
        operation: &AlterTableOperation,
    ) -> DMSRResult<()> {
        debug!("Parsing alter table statement: {:?}", operation);
        let full_table_name = format!("{}.{}", schema, table);

        match operation {
            AlterTableOperation::AddColumn { column_def, .. } => {
                self.parse_add_column_statement(&full_table_name, column_def)?
            }
            AlterTableOperation::AlterColumn {
                column_name, op, ..
            } => self.parse_alter_column_statement(&full_table_name, column_name, op)?,
            AlterTableOperation::DropColumn { column_name, .. } => {
                self.parse_drop_column_statement(&full_table_name, column_name)?
            }
            AlterTableOperation::DropPrimaryKey => {
                self.parse_drop_primary_key_statement(&full_table_name)?
            }
            AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
                ..
            } => self.parse_rename_column_statement(
                &full_table_name,
                old_column_name,
                new_column_name,
            )?,
            AlterTableOperation::RenameTable { table_name } => {
                self.parse_rename_table_statement(&full_table_name, schema, table_name)?
            }
            _ => {
                return Err(DMSRError::MySQLSourceConnectorError(
                    format!(
                        "Alter table operation not supported by parser: {}",
                        operation
                    )
                    .into(),
                ))
            }
        };

        Ok(())
    }

    fn parse_drop_statement(
        &mut self,
        schema: &str,
        object_type: &ObjectType,
        names: &[ObjectName],
    ) -> DMSRResult<Vec<(String, String)>> {
        if object_type != &ObjectType::Table {
            return Ok(vec![]);
        }

        let mut dropped_tables = vec![];
        for n in names {
            let (schema, name) = Self::parse_schema_table_from_sqlparser(schema, n)?;
            let full_table_name = format!("{}.{}", schema, name);
            debug!("Dropping table: {}", full_table_name);
            self.table_metadata_map.remove(&full_table_name);
            dropped_tables.push((schema, name));
        }

        Ok(dropped_tables)
    }

    fn parse_truncate_statement(
        &self,
        schema: &str,
        table: &str,
        ts: u64,
        log_pos: u64,
    ) -> DMSRResult<ConnectorOutput> {
        let msg_metadata = MySQLSourceMetadata::new(
            &self.connector_name,
            &self.config.db,
            schema,
            table,
            self.config.server_id,
            self.binlog_file_name()?,
            log_pos,
        );

        let output = MySQLRowDataOutput::new(None, None, Operation::Truncate, ts, msg_metadata);
        let output = ConnectorOutput::MySQLRowData(output);

        Ok(output)
    }

    fn parse_add_column_statement(
        &mut self,
        full_table_name: &str,
        column_def: &ColumnDef,
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

        let table = self.table_meta_as_mut(full_table_name)?;
        table.columns.push(MySQLTableColumn {
            column_name,
            data_type,
            is_nullable: is_nullable && !is_primary_key,
            is_primary_key,
        });

        Ok(())
    }

    fn parse_alter_column_statement(
        &mut self,
        full_table_name: &str,
        column_name: &Ident,
        op: &AlterColumnOperation,
    ) -> DMSRResult<()> {
        let column_name = column_name.to_string();
        let col = self
            .table_meta_as_mut(full_table_name)?
            .column_as_mut(&column_name)?;

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

    fn parse_drop_column_statement(
        &mut self,
        full_table_name: &str,
        column_name: &Ident,
    ) -> DMSRResult<()> {
        let column_name = column_name.to_string();
        let table = self.table_meta_as_mut(full_table_name)?;
        table.columns = table
            .columns
            .clone()
            .into_iter()
            .filter(|c| c.column_name != column_name)
            .collect();
        Ok(())
    }

    fn parse_drop_primary_key_statement(&mut self, full_table_name: &str) -> DMSRResult<()> {
        let table = self.table_meta_as_mut(full_table_name)?;
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

    fn parse_rename_column_statement(
        &mut self,
        full_table_name: &str,
        old_column_name: &Ident,
        new_column_name: &Ident,
    ) -> DMSRResult<()> {
        let old_column_name = old_column_name.to_string();
        let new_column_name = new_column_name.to_string();
        let table = self.table_meta_as_mut(full_table_name)?;
        let col = table.column_as_mut(&old_column_name)?;
        col.column_name = new_column_name;
        Ok(())
    }

    fn parse_rename_table_statement(
        &mut self,
        old_full_table_name: &str,
        schema: &str,
        new_table_name: &ObjectName,
    ) -> DMSRResult<()> {
        let (schema, new_table_name) =
            Self::parse_schema_table_from_sqlparser(schema, new_table_name)?;

        let new_full_table_name = format!("{}.{}", schema, new_table_name);
        //
        // let table = self.table_meta_as_mut(old_full_table_name)?;
        // table.table_name = new_table_name.to_string();
        // self.table_metadata_map.insert(new_full_table_name, table.clone());
        // self.table_metadata_map.remove(old_full_table_name);
        Ok(())
    }

    fn parse_schema_table_from_sqlparser(
        event_schema: &str,
        object_name: &ObjectName,
    ) -> DMSRResult<(String, String)> {
        let ident: &Vec<Ident> = &object_name.0;
        debug!("parse_schema_table_from_sqlparser: {:?}", ident);
        match ident.len() {
            1 => Ok((event_schema.to_string(), ident[0].value.clone())),
            2 => Ok((ident[0].value.clone(), ident[1].value.clone())),
            _ => Err(DMSRError::MySQLSourceConnectorError(
                format!("Invalid table name: {:?}", object_name).into(),
            )),
        }
    }

    fn parse_schema_table_from_table_map_event(
        event: &TableMapEvent,
    ) -> DMSRResult<(String, String)> {
        let mut schema = event.database_name().to_string();
        let mut table_name = event.table_name().to_string();

        let split = table_name.split('.').collect::<Vec<&str>>();
        if split.len() == 2 {
            schema = split[0].to_string();
            table_name = split[1].to_string();
        }

        Ok((schema, table_name))
    }

    fn table_meta_as_mut(&mut self, full_table_name: &str) -> DMSRResult<&mut MySQLTable> {
        self.table_metadata_map.get_mut(full_table_name).ok_or(
            DMSRError::MySQLSourceConnectorError(
                format!("Table not found: {}", full_table_name).into(),
            ),
        )
    }

    fn table_meta_as_ref(&self, full_table_name: &str) -> DMSRResult<&MySQLTable> {
        self.table_metadata_map
            .get(full_table_name)
            .ok_or(DMSRError::MySQLSourceConnectorError(
                format!("Table not found: {}", full_table_name).into(),
            ))
    }

    fn last_table_map_event_as_ref(&self) -> DMSRResult<&TableMapEvent> {
        self.last_table_map_event
            .as_ref()
            .ok_or(DMSRError::MySQLSourceConnectorError(
                "No table event found".into(),
            ))
    }

    fn binlog_file_name(&self) -> DMSRResult<&str> {
        let binlog_file_name =
            self.binlog_file_name
                .as_ref()
                .ok_or(DMSRError::MySQLSourceConnectorError(
                    "No binlog file name found".into(),
                ))?;
        Ok(binlog_file_name)
    }

    pub fn set_binlog_file_name(&mut self, binlog_file_name: String) {
        self.binlog_file_name = Some(binlog_file_name);
    }
}
