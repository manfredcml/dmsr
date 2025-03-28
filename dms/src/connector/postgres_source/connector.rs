// use crate::connector::connector::SourceConnector;
// use crate::connector::kind::ConnectorKind;
// use crate::connector::postgres_source::config::PostgresSourceConfig;
// use crate::decoding::pgoutput::events::{
//     ColumnData, DeleteEvent, InsertEvent, PgOutputEvent, RelationColumn, RelationEvent,
//     TruncateEvent, UpdateEvent,
// };
// use crate::decoding::pgoutput::utils::{keep_alive, parse_pgoutput_event};
// use crate::error::error::{DMSRError, DMSRResult};
// use crate::kafka::kafka::Kafka;
// use crate::message::postgres_source::PostgresSource;
// use crate::types::postgres_types::PostgresKafkaConnectTypeMap;
// use async_trait::async_trait;
// use bytes::Bytes;
// use futures::StreamExt;
// use serde_json::{json, Value};
// use std::collections::HashMap;
// use std::pin::Pin;
// use tokio_postgres::{Client, CopyBothDuplex, NoTls, SimpleQueryMessage, SimpleQueryRow};
//
// pub struct PostgresSourceConnector {
//     config: PostgresSourceConfig,
//     client: Client,
//     connector_name: String,
//     relation_map: HashMap<u32, RelationEvent>,
//     pg_oid_type_map: HashMap<u32, String>,
//     pg_kafka_connect_type_map: PostgresKafkaConnectTypeMap,
// }
//
// #[async_trait]
// impl SourceConnector for PostgresSourceConnector {
//     type Config = PostgresSourceConfig;
//
//     async fn new(connector_name: String, config: &PostgresSourceConfig) -> DMSRResult<Box<Self>> {
//         let client = Self::connect(config).await?;
//         Ok(Box::new(PostgresSourceConnector {
//             config: config.clone(),
//             client,
//             connector_name,
//             relation_map: HashMap::new(),
//             pg_oid_type_map: HashMap::new(),
//             pg_kafka_connect_type_map: PostgresKafkaConnectTypeMap::new(),
//         }))
//     }
//
//     async fn cdc_events_to_stream(&mut self) -> DMSRResult<()> {
//         self.get_oid_type_map().await?;
//         self.create_publication().await?;
//         let (lsn, slot_name) = self.create_replication_slot().await?;
//         let mut stream = self.start_replication_slot(lsn, slot_name).await?;
//
//         let mut tx_id: Option<u32> = None;
//         while let Some(event) = stream.next().await {
//             let event = event?;
//             let event = event.as_ref();
//
//             match &event[0] {
//                 b'k' => {
//                     keep_alive(event, &mut stream).await?;
//                 }
//                 b'w' => {
//                     let pg_output_event = parse_pgoutput_event(event)?;
//                     let mut kafka_message: Vec<KafkaJSONMessage<PostgresSource>> = vec![];
//
//                     match pg_output_event {
//                         PgOutputEvent::Begin(event) => {
//                             println!("Begin: {:?}", event);
//                             tx_id = Some(event.tx_xid);
//                         }
//                         PgOutputEvent::Commit(event) => {
//                             println!("Commit: {:?}", event);
//                             tx_id = None;
//                         }
//                         PgOutputEvent::Relation(event) => {
//                             println!("Relation: {:?}", event);
//                             self.process_relation_event(event).await?;
//                         }
//                         PgOutputEvent::Insert(event) => {
//                             println!("Insert: {:?}", event);
//                             kafka_message = self.process_insert_event(event, tx_id).await?;
//                         }
//                         PgOutputEvent::Update(event) => {
//                             println!("Update: {:?}", event);
//                             kafka_message = self.process_update_event(event, tx_id).await?;
//                         }
//                         PgOutputEvent::Delete(event) => {
//                             println!("Delete: {:?}", event);
//                             kafka_message = self.process_delete_event(event, tx_id).await?;
//                         }
//                         PgOutputEvent::Truncate(event) => {
//                             println!("Truncate: {:?}", event);
//                             kafka_message = self.process_truncate_event(event, tx_id).await?;
//                         }
//                         PgOutputEvent::Origin => {}
//                     }
//
//                     for msg in kafka_message {
//                         kafka.ingest("test-topic".to_string(), &msg, None).await?;
//                     }
//
//                     println!("====================================")
//                 }
//                 _ => {}
//             }
//         }
//
//         Ok(())
//     }
// }
//
// impl PostgresSourceConnector {
//     async fn connect(config: &PostgresSourceConfig) -> DMSRResult<Client> {
//         let endpoint = format!(
//             "host={} port={} user={} password={} dbname={} replication=database",
//             config.host, config.port, config.user, config.password, config.db
//         );
//
//         let (client, connection) = tokio_postgres::connect(endpoint.as_str(), NoTls).await?;
//
//         tokio::spawn(async move {
//             if let Err(e) = connection.await {
//                 eprintln!("connection error: {}", e);
//             }
//         });
//
//         Ok(client)
//     }
//
//     async fn execute_query(&self, query: String) -> DMSRResult<Vec<SimpleQueryRow>> {
//         let res = self.client.simple_query(query.as_str()).await?;
//
//         let res = res
//             .into_iter()
//             .filter_map(|m| match m {
//                 SimpleQueryMessage::Row(r) => Some(r),
//                 _ => None,
//             })
//             .collect::<Vec<SimpleQueryRow>>();
//
//         Ok(res)
//     }
//
//     async fn get_oid_type_map(&mut self) -> DMSRResult<()> {
//         let query = "SELECT oid, typname FROM pg_type";
//         let res = self.execute_query(query.into()).await?;
//
//         let mut map = HashMap::new();
//         for row in res {
//             let oid = row.get(0).ok_or(DMSRError::PostgresError("oid".into()))?;
//             let oid: u32 = oid.parse()?;
//
//             let type_name = row
//                 .get(1)
//                 .ok_or(DMSRError::PostgresError("typname".into()))?
//                 .to_string();
//
//             map.insert(oid, type_name);
//         }
//
//         self.pg_oid_type_map = map;
//
//         Ok(())
//     }
//
//     async fn create_publication(&self) -> DMSRResult<()> {
//         let publication_name = format!("dmsr_pub_all_tables_{}", self.connector_name);
//         let query = format!("CREATE PUBLICATION {} FOR ALL TABLES", publication_name);
//
//         let res = self.execute_query(query).await;
//         if res.is_err() {
//             println!("Publication already exists");
//         }
//
//         Ok(())
//     }
//
//     async fn create_replication_slot(&self) -> DMSRResult<(String, String)> {
//         let slot_name = format!("dmsr_slot_{}", self.connector_name);
//         let slot_query = format!("CREATE_REPLICATION_SLOT {} LOGICAL \"pgoutput\"", slot_name);
//         let slot_query_res = self.execute_query(slot_query).await;
//
//         let lsn: String;
//         if let Ok(res) = slot_query_res {
//             lsn = res[0]
//                 .get("consistent_point")
//                 .ok_or(DMSRError::PostgresError("No LSN found".into()))?
//                 .to_string();
//         } else {
//             let find_slot_query = format!(
//                 "SELECT * FROM pg_replication_slots WHERE slot_name = '{}'",
//                 slot_name
//             );
//
//             let find_slot_res = self.execute_query(find_slot_query).await?;
//             lsn = find_slot_res[0]
//                 .get("confirmed_flush_lsn")
//                 .ok_or(DMSRError::PostgresError("No LSN found".into()))?
//                 .to_string();
//         }
//
//         Ok((lsn, slot_name))
//     }
//
//     async fn start_replication_slot(
//         &mut self,
//         lsn: String,
//         slot_name: String,
//     ) -> DMSRResult<Pin<Box<CopyBothDuplex<Bytes>>>> {
//         let pub_name = format!("dmsr_pub_all_tables_{}", self.connector_name);
//         let query = format!(
//             "START_REPLICATION SLOT {} LOGICAL {} (\"proto_version\" '1', \"publication_names\" '{}')",
//             slot_name,
//             lsn,
//             pub_name
//         );
//
//         let stream = self.client.copy_both_simple::<Bytes>(&query).await?;
//         let stream = Box::pin(stream);
//
//         Ok(stream)
//     }
//
//     fn get_db_fields(&self, columns: &[RelationColumn]) -> Vec<KafkaJSONField> {
//         columns
//             .iter()
//             .map(|c| {
//                 let pg_col_type = self.pg_oid_type_map.get(&c.column_type);
//                 let unknown = String::from("unknown");
//                 let col_type = self
//                     .pg_kafka_connect_type_map
//                     .get(pg_col_type.unwrap_or(&unknown))
//                     .unwrap_or(unknown);
//                 KafkaJSONField {
//                     r#type: col_type,
//                     optional: !c.is_pk,
//                     field: c.column_name.clone(),
//                     fields: None,
//                 }
//             })
//             .collect::<Vec<KafkaJSONField>>()
//     }
//
//     fn get_field_values(&self, fields: &[KafkaJSONField], columns: &[ColumnData]) -> Value {
//         let mut values: Value = json!({});
//
//         for (f, c) in fields.iter().zip(columns.iter()) {
//             let field_name = f.field.clone();
//             let field_value = c.column_value.clone();
//             values[field_name] = json!(field_value);
//         }
//
//         values
//     }
//
//     fn extract_meta_from_relation_id(
//         &self,
//         relation_id: u32,
//     ) -> DMSRResult<(KafkaJSONSchema, Vec<KafkaJSONField>, String, String)> {
//         let relation_event = self
//             .relation_map
//             .get(&relation_id)
//             .ok_or(DMSRError::PostgresError("Relation not found".into()))?;
//
//         let db_fields = self.get_db_fields(&relation_event.columns);
//         let default_fields = self.get_default_fields(&db_fields);
//         let msg_schema = self.get_message_schema(relation_event, &default_fields);
//         let schema_name = relation_event.namespace.clone();
//         let table_name = relation_event.relation_name.clone();
//         Ok((msg_schema, db_fields, schema_name, table_name))
//     }
//
//     fn get_default_fields(&self, db_fields: &[KafkaJSONField]) -> Vec<KafkaJSONField> {
//         let before = KafkaJSONField {
//             r#type: "struct".into(),
//             optional: true,
//             field: "before".into(),
//             fields: Some(db_fields.to_vec()),
//         };
//
//         let after = KafkaJSONField {
//             r#type: "struct".into(),
//             optional: true,
//             field: "after".into(),
//             fields: Some(db_fields.to_vec()),
//         };
//
//         let op = KafkaJSONField {
//             r#type: "string".into(),
//             optional: false,
//             field: "op".into(),
//             fields: None,
//         };
//
//         let ts_ms = KafkaJSONField {
//             r#type: "int64".into(),
//             optional: true,
//             field: "ts_ms".into(),
//             fields: None,
//         };
//
//         let source = KafkaJSONField {
//             r#type: "struct".into(),
//             optional: false,
//             field: "source".into(),
//             fields: Some(vec![
//                 KafkaJSONField {
//                     r#type: "string".into(),
//                     optional: false,
//                     field: "connector_type".into(),
//                     fields: None,
//                 },
//                 KafkaJSONField {
//                     r#type: "string".into(),
//                     optional: false,
//                     field: "connector_name".into(),
//                     fields: None,
//                 },
//                 KafkaJSONField {
//                     r#type: "int64".into(),
//                     optional: false,
//                     field: "lsn".into(),
//                     fields: None,
//                 },
//                 KafkaJSONField {
//                     r#type: "string".into(),
//                     optional: false,
//                     field: "db".into(),
//                     fields: None,
//                 },
//                 KafkaJSONField {
//                     r#type: "string".into(),
//                     optional: false,
//                     field: "schema".into(),
//                     fields: None,
//                 },
//                 KafkaJSONField {
//                     r#type: "string".into(),
//                     optional: false,
//                     field: "table".into(),
//                     fields: None,
//                 },
//                 KafkaJSONField {
//                     r#type: "int64".into(),
//                     optional: false,
//                     field: "tx_id".into(),
//                     fields: None,
//                 },
//             ]),
//         };
//
//         vec![before, after, op, ts_ms, source]
//     }
//
//     fn get_message_schema(
//         &self,
//         relation_event: &RelationEvent,
//         default_fields: &[KafkaJSONField],
//     ) -> KafkaJSONSchema {
//         let schema_name = format!(
//             "{}.{}",
//             &relation_event.namespace, &relation_event.relation_name
//         );
//
//         KafkaJSONSchema {
//             r#type: "struct".to_string(),
//             fields: default_fields.to_vec(),
//             optional: false,
//             name: schema_name,
//         }
//     }
//
//     fn get_message_source(
//         &self,
//         lsn: u64,
//         schema_name: String,
//         table_name: String,
//         tx_id: u32,
//     ) -> PostgresSource {
//         PostgresSource {
//             connector_type: ConnectorKind::PostgresSource,
//             connector_name: self.connector_name.clone(),
//             lsn,
//             db: self.config.db.clone(),
//             schema: schema_name,
//             table: table_name,
//             tx_id,
//         }
//     }
//
//     async fn process_relation_event(&mut self, event: RelationEvent) -> DMSRResult<()> {
//         self.relation_map.insert(event.relation_id, event);
//         Ok(())
//     }
//
//     async fn process_insert_event(
//         &self,
//         event: InsertEvent,
//         tx_id: Option<u32>,
//     ) -> DMSRResult<Vec<KafkaJSONMessage<PostgresSource>>> {
//         let tx_id = tx_id.ok_or(DMSRError::PostgresError("No transaction id".into()))?;
//         let relation_id = event.relation_id;
//         let (schema, db_fields, schema_name, table_name) =
//             self.extract_meta_from_relation_id(relation_id)?;
//         let db_values = self.get_field_values(&db_fields, &event.columns);
//         let source = self.get_message_source(event.lsn, schema_name, table_name, tx_id);
//
//         let payload = KafkaJSONPayload {
//             before: None,
//             after: Some(db_values),
//             op: Operation::Create,
//             ts_ms: event.timestamp.timestamp_millis().try_into()?,
//             metadata: source,
//         };
//
//         Ok(vec![KafkaJSONMessage { schema, payload }])
//     }
//
//     async fn process_update_event(
//         &self,
//         event: UpdateEvent,
//         tx_id: Option<u32>,
//     ) -> DMSRResult<Vec<KafkaJSONMessage<PostgresSource>>> {
//         let tx_id = tx_id.ok_or(DMSRError::PostgresError("No transaction id".into()))?;
//         let relation_id = event.relation_id;
//         let (schema, db_fields, schema_name, table_name) =
//             self.extract_meta_from_relation_id(relation_id)?;
//         let db_values = self.get_field_values(&db_fields, &event.columns);
//
//         let source = self.get_message_source(event.lsn, schema_name, table_name, tx_id);
//
//         let payload = KafkaJSONPayload {
//             before: None,
//             after: Some(db_values),
//             op: Operation::Update,
//             ts_ms: event.timestamp.timestamp_millis().try_into()?,
//             metadata: source,
//         };
//
//         Ok(vec![KafkaJSONMessage { schema, payload }])
//     }
//
//     async fn process_delete_event(
//         &self,
//         event: DeleteEvent,
//         tx_id: Option<u32>,
//     ) -> DMSRResult<Vec<KafkaJSONMessage<PostgresSource>>> {
//         let tx_id = tx_id.ok_or(DMSRError::PostgresError("No transaction id".into()))?;
//         let relation_id = event.relation_id;
//         let (schema, db_fields, schema_name, table_name) =
//             self.extract_meta_from_relation_id(relation_id)?;
//         let db_values = self.get_field_values(&db_fields, &event.columns);
//
//         let source = self.get_message_source(event.lsn, schema_name, table_name, tx_id);
//
//         let payload = KafkaJSONPayload {
//             before: Some(db_values),
//             after: None,
//             op: Operation::Delete,
//             ts_ms: event.timestamp.timestamp_millis().try_into()?,
//             metadata: source,
//         };
//
//         Ok(vec![KafkaJSONMessage { schema, payload }])
//     }
//
//     async fn process_truncate_event(
//         &self,
//         event: TruncateEvent,
//         tx_id: Option<u32>,
//     ) -> DMSRResult<Vec<KafkaJSONMessage<PostgresSource>>> {
//         let tx_id = tx_id.ok_or(DMSRError::PostgresError("No transaction id".into()))?;
//         let mut messages: Vec<KafkaJSONMessage<PostgresSource>> = vec![];
//         for relation_id in event.relation_ids.iter() {
//             let (schema, _, schema_name, table_name) =
//                 self.extract_meta_from_relation_id(*relation_id)?.clone();
//
//             let source = self.get_message_source(event.lsn, schema_name, table_name, tx_id);
//
//             let payload: KafkaJSONPayload<PostgresSource> = KafkaJSONPayload {
//                 before: None,
//                 after: None,
//                 op: Operation::Truncate,
//                 ts_ms: event.timestamp.timestamp_millis().try_into()?,
//                 metadata: source,
//             };
//
//             messages.push(KafkaJSONMessage { schema, payload });
//         }
//
//         Ok(messages)
//     }
// }
