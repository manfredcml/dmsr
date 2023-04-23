use crate::connector::kind::ConnectorKind;
use crate::connector::mysql_source::config::MySQLSourceConfig;
use crate::connector::mysql_source::metadata::MySQLSourceMetadata;
use crate::connector::mysql_source::table::{MySQLTable, MySQLTableColumn};
use crate::error::error::{DMSRError, DMSRResult};
use crate::kafka::json::{
    JSONDDLMessage, JSONDDLPayload, JSONMessage, JSONPayload, JSONSchema, Operation,
};
use crate::kafka::message::KafkaMessage;
use log::{debug, error};
use mysql_async::binlog::events::{
    DeleteRowsEvent, Event, QueryEvent, RotateEvent, RowsEventRows, TableMapEvent, UpdateRowsEvent,
    WriteRowsEvent,
};
use mysql_async::binlog::row::BinlogRow;
use mysql_async::binlog::value::BinlogValue;
use mysql_async::binlog::EventType;
use mysql_async::Value;
use serde_json::json;
use sqlparser::ast::{
    AlterColumnOperation, AlterTableOperation, ColumnDef, ColumnOption, Ident, ObjectName,
    ObjectType, Statement, TableConstraint,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;

enum RowsEvent<'a> {
    Write(WriteRowsEvent<'a>),
    Update(UpdateRowsEvent<'a>),
    Delete(DeleteRowsEvent<'a>),
}

#[derive(Debug, Clone)]
pub struct MySQLDecoder {
    table_metadata_map: HashMap<String, MySQLTable>,
    last_table_map_event: Option<TableMapEvent<'static>>,
    binlog_file_name: Option<String>,
    connector_name: String,
    connector_config: MySQLSourceConfig,
}

impl MySQLDecoder {
    pub fn new(connector_name: String, connector_config: MySQLSourceConfig) -> Self {
        MySQLDecoder {
            table_metadata_map: HashMap::new(),
            last_table_map_event: None,
            binlog_file_name: None,
            connector_name,
            connector_config,
        }
    }

    pub fn parse(&mut self, event: Event) -> DMSRResult<Vec<KafkaMessage>> {
        let ts = 1000 * (event.fde().create_timestamp() as u64);
        let log_pos = event.header().log_pos() as u64;
        let event_type = event.header().event_type()?;

        let mut kafka_messages: Vec<KafkaMessage> = vec![];
        match &event_type {
            EventType::QUERY_EVENT => {
                let event = &event.read_event::<QueryEvent>()?;
                if let Ok(message) = self.parse_query_event(event, ts, log_pos) {
                    kafka_messages = message;
                }
            }
            EventType::TABLE_MAP_EVENT => {
                let event = event.read_event::<TableMapEvent>()?;
                let result = self.parse_table_map_event(event.into_owned());
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
                        kafka_messages = messages;
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

        Ok(kafka_messages)
    }

    fn parse_rows_event(
        &mut self,
        event: &RowsEvent,
        ts: u64,
        log_pos: u64,
    ) -> DMSRResult<Vec<KafkaMessage>> {
        let last_table_map_event = self.last_table_map_event_as_ref()?;

        let (schema, table_name) =
            Self::parse_schema_table_from_table_map_event(last_table_map_event)?;

        let full_table_name = format!("{}.{}", schema, table_name);

        let table_metadata = self.table_meta_as_ref(&full_table_name)?;
        let table_columns = &table_metadata.columns;
        let num_columns = table_columns.len();

        let kafka_messages = match event {
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

        let kafka_messages: Vec<KafkaMessage> = kafka_messages
            .iter()
            .filter_map(|m| {
                let metadata = &m.payload.metadata;
                let topic = format!(
                    "{}-{}.{}",
                    &self.connector_name, metadata.db, metadata.table
                );
                m.to_kafka_message(topic, None).ok()
            })
            .collect();

        Ok(kafka_messages)
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
    ) -> DMSRResult<Vec<JSONMessage<MySQLSourceMetadata>>> {
        let mut kafka_messages: Vec<JSONMessage<MySQLSourceMetadata>> = vec![];

        for (before, after) in rows_data {
            let full_table_name = format!(
                "{}.{}",
                table_metadata.schema_name, table_metadata.table_name
            );
            let msg_schema = table_metadata.as_json_message_schema(&full_table_name)?;

            let msg_source = MySQLSourceMetadata::new(
                &self.connector_name,
                &self.connector_config.db,
                &table_metadata.schema_name,
                &table_metadata.table_name,
                self.connector_config.server_id,
                self.binlog_file_name()?,
                log_pos,
            );

            let msg_payload: JSONPayload<MySQLSourceMetadata> =
                JSONPayload::new(Some(before), Some(after), op.clone(), ts, msg_source);

            kafka_messages.push(JSONMessage::new(msg_schema, msg_payload));
        }

        Ok(kafka_messages)
    }

    fn parse_rotate_event(&mut self, event: &RotateEvent) -> DMSRResult<()> {
        self.binlog_file_name = Some(event.name().to_string());
        Ok(())
    }

    fn parse_table_map_event(&mut self, event: TableMapEvent<'static>) -> DMSRResult<()> {
        self.last_table_map_event = Some(event);
        Ok(())
    }

    fn parse_query_event(
        &mut self,
        event: &QueryEvent,
        ts: u64,
        log_pos: u64,
    ) -> DMSRResult<Vec<KafkaMessage>> {
        debug!("QUERY_EVENT: {:?}\n", event);

        let schema = event.schema().to_string();
        let query = event.query().to_string();
        let dialect = MySqlDialect {};
        let ast: Vec<Statement> = Parser::parse_sql(&dialect, &query)?;

        let stmt = ast.first().ok_or(DMSRError::MySQLSourceConnectorError(
            format!("Query not supported by parser: {}", query).into(),
        ))?;
        debug!("SQL statement: {:?}\n", stmt);

        let kafka_message = match stmt {
            Statement::CreateTable {
                name,
                columns,
                constraints,
                ..
            } => {
                let (schema, table) = Self::parse_schema_table_from_sqlparser(&schema, name)?;
                self.parse_create_table_statement(&schema, &table, columns, constraints)?;
                let msg =
                    self.query_event_to_kafka_message(&schema, &table, &query, ts, log_pos)?;
                Ok(vec![msg])
            }
            Statement::AlterTable {
                name, operation, ..
            } => {
                let (schema, table) = Self::parse_schema_table_from_sqlparser(&schema, name)?;
                self.parse_alter_table_statement(&schema, &table, operation)?;
                let msg =
                    self.query_event_to_kafka_message(&schema, &table, &query, ts, log_pos)?;
                Ok(vec![msg])
            }
            Statement::Drop {
                object_type, names, ..
            } => {
                let dropped_tables = self.parse_drop_statement(&schema, object_type, names)?;

                let msg: Vec<KafkaMessage> = dropped_tables
                    .iter()
                    .filter_map(|(schema, table)| {
                        self.query_event_to_kafka_message(schema, table, &query, ts, log_pos)
                            .ok()
                    })
                    .collect();

                Ok(msg)
            }
            Statement::Truncate { table_name, .. } => {
                let (schema, table) = Self::parse_schema_table_from_sqlparser(&schema, table_name)?;
                let msg = self.parse_truncate_statement(&schema, &table, ts, log_pos)?;
                Ok(vec![msg])
            }
            _ => Err(DMSRError::MySQLSourceConnectorError(
                format!("Query not supported by parser: {}", query).into(),
            )),
        };

        kafka_message
    }

    fn query_event_to_kafka_message(
        &self,
        schema: &str,
        table: &str,
        ddl: &str,
        ts: u64,
        log_pos: u64,
    ) -> DMSRResult<KafkaMessage> {
        let metadata = MySQLSourceMetadata::new(
            &self.connector_name,
            &self.connector_config.db,
            schema,
            table,
            self.connector_config.server_id,
            self.binlog_file_name()?,
            log_pos,
        );

        let schema = JSONSchema::new_ddl(
            MySQLSourceMetadata::get_schema(),
            &ConnectorKind::MySQLSource.to_string(),
        );

        let payload: JSONDDLPayload<MySQLSourceMetadata> = JSONDDLPayload::new(ddl, ts, metadata);

        let json_message = JSONDDLMessage::new(schema, payload);

        json_message.to_kafka_message(
            format!("{}-{}", self.connector_name, self.connector_config.db),
            None,
        )
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
                self.parse_rename_table_statement(&full_table_name, &schema, table_name)?
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
    ) -> DMSRResult<KafkaMessage> {
        let full_table_name = format!("{}.{}", schema, table);
        let table_meta = self.table_meta_as_ref(&full_table_name)?;
        let msg_schema = table_meta.as_json_message_schema(&full_table_name)?;
        let msg_metadata = MySQLSourceMetadata::new(
            &self.connector_name,
            &self.connector_config.db,
            schema,
            table,
            self.connector_config.server_id,
            self.binlog_file_name()?,
            log_pos,
        );
        let msg_payload: JSONPayload<MySQLSourceMetadata> =
            JSONPayload::new(None, None, Operation::Truncate, ts, msg_metadata);

        let msg = JSONMessage::new(msg_schema, msg_payload);
        let topic = format!("{}-{}", self.connector_name, full_table_name);
        let msg = msg.to_kafka_message(topic, None)?;
        Ok(msg)
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
        match ident.len() {
            1 => Ok((event_schema.to_string(), ident[0].to_string())),
            2 => Ok((ident[0].to_string(), ident[1].to_string())),
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
}
