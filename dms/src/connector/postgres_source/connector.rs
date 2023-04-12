use crate::connector::connector::Connector;
use crate::connector::postgres_source::config::PostgresSourceConfig;
use crate::connector::postgres_source::event::{Action, Column, RawPostgresEvent};
use crate::connector::postgres_source::pgoutput::events::{
    ColumnData, InsertEvent, PgOutputEvent, RelationColumn, RelationEvent,
};
use crate::connector::postgres_source::pgoutput::utils::{keep_alive, parse_pgoutput_event};
use crate::error::error::{DMSRError, DMSRResult};
use crate::kafka::kafka::Kafka;
use crate::message::message::{Field, KafkaMessage, Payload, Schema};
use crate::message::postgres_types::PostgresKafkaConnectTypeMap;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use mysql_common::frunk::labelled::chars::F;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::os::unix::raw::ino_t;
use std::pin::Pin;
use tokio_postgres::{Client, CopyBothDuplex, NoTls, SimpleQueryMessage, SimpleQueryRow};

pub struct PostgresSourceConnector {
    config: PostgresSourceConfig,
    client: Client,
    connector_name: String,
    relation_map: HashMap<u32, RelationEvent>,
    pg_oid_type_map: HashMap<u32, String>,
    pg_kafka_connect_type_map: PostgresKafkaConnectTypeMap,
}

#[async_trait]
impl Connector for PostgresSourceConnector {
    type Config = PostgresSourceConfig;

    async fn new(connector_name: String, config: &PostgresSourceConfig) -> DMSRResult<Box<Self>> {
        let client = Self::connect(config).await?;
        Ok(Box::new(PostgresSourceConnector {
            config: config.clone(),
            client,
            connector_name,
            relation_map: HashMap::new(),
            pg_oid_type_map: HashMap::new(),
            pg_kafka_connect_type_map: PostgresKafkaConnectTypeMap::new(),
        }))
    }

    async fn stream(&mut self, kafka: &Kafka) -> DMSRResult<()> {
        self.get_oid_type_map().await?;
        self.create_publication().await?;
        let (lsn, slot_name) = self.create_replication_slot().await?;
        let mut stream = self.start_replication_slot(lsn, slot_name).await?;

        while let Some(event) = stream.next().await {
            let event = event?;
            let event = event.as_ref();

            match &event[0] {
                b'k' => {
                    keep_alive(event, &mut stream).await?;
                }
                b'w' => {
                    let pg_output_event = parse_pgoutput_event(event)?;

                    match pg_output_event {
                        PgOutputEvent::Begin(event) => {
                            println!("Begin: {:?}", event);
                        }
                        PgOutputEvent::Commit(event) => {
                            println!("Commit: {:?}", event);
                        }
                        PgOutputEvent::Relation(event) => {
                            println!("Relation: {:?}", event);
                            self.relation_map.insert(event.relation_id, event);
                        }
                        PgOutputEvent::Insert(event) => {
                            println!("Insert: {:?}", event);
                            self.process_insert_event(event, kafka).await?;
                        }
                        PgOutputEvent::Update(event) => {
                            println!("Update: {:?}", event);
                        }
                        PgOutputEvent::Delete(event) => {
                            println!("Delete: {:?}", event);
                        }
                        PgOutputEvent::Truncate(event) => {
                            println!("Truncate: {:?}", event);
                        }
                        PgOutputEvent::Origin => {}
                    }

                    println!("====================================")
                }
                _ => {}
            }
        }

        Ok(())
    }
}

impl PostgresSourceConnector {
    async fn connect(config: &PostgresSourceConfig) -> DMSRResult<Client> {
        let endpoint = format!(
            "host={} port={} user={} password={} replication=database",
            config.host, config.port, config.user, config.password
        );

        let (client, connection) = tokio_postgres::connect(endpoint.as_str(), NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        Ok(client)
    }

    async fn execute_query(&self, query: String) -> DMSRResult<Vec<SimpleQueryRow>> {
        let res = self.client.simple_query(query.as_str()).await?;

        let res = res
            .into_iter()
            .filter_map(|m| match m {
                SimpleQueryMessage::Row(r) => Some(r),
                _ => None,
            })
            .collect::<Vec<SimpleQueryRow>>();

        Ok(res)
    }

    async fn get_oid_type_map(&mut self) -> DMSRResult<()> {
        let query = "SELECT oid, typname FROM pg_type";
        let res = self.execute_query(query.into()).await?;

        let mut map = HashMap::new();
        for row in res {
            let oid = row.get(0).ok_or(DMSRError::PostgresError("oid".into()))?;
            let oid: u32 = oid.parse()?;

            let type_name = row
                .get(1)
                .ok_or(DMSRError::PostgresError("typname".into()))?
                .to_string();

            map.insert(oid, type_name);
        }

        self.pg_oid_type_map = map;

        Ok(())
    }

    async fn create_publication(&self) -> DMSRResult<()> {
        let publication_name = format!("dmsr_pub_all_tables_{}", self.connector_name);
        let query = format!("CREATE PUBLICATION {} FOR ALL TABLES", publication_name);

        let res = self.execute_query(query).await;
        if res.is_err() {
            println!("Publication already exists");
        }

        Ok(())
    }

    async fn create_replication_slot(&self) -> DMSRResult<(String, String)> {
        let slot_name = format!("dmsr_slot_{}", self.connector_name);
        let slot_query = format!("CREATE_REPLICATION_SLOT {} LOGICAL \"pgoutput\"", slot_name);
        let slot_query_res = self.execute_query(slot_query).await;

        let lsn: String;
        if let Ok(res) = slot_query_res {
            lsn = res[0]
                .get("consistent_point")
                .ok_or(DMSRError::PostgresError("No LSN found".into()))?
                .to_string();
        } else {
            let find_slot_query = format!(
                "SELECT * FROM pg_replication_slots WHERE slot_name = '{}'",
                slot_name
            );

            let find_slot_res = self.execute_query(find_slot_query).await?;
            lsn = find_slot_res[0]
                .get("confirmed_flush_lsn")
                .ok_or(DMSRError::PostgresError("No LSN found".into()))?
                .to_string();
        }

        Ok((lsn, slot_name))
    }

    async fn start_replication_slot(
        &mut self,
        lsn: String,
        slot_name: String,
    ) -> DMSRResult<Pin<Box<CopyBothDuplex<Bytes>>>> {
        let query = format!(
            "START_REPLICATION SLOT {} LOGICAL {} (\"proto_version\" '1', \"publication_names\" 'all_tables')",
            slot_name,
            lsn
        );

        let stream = self.client.copy_both_simple::<Bytes>(&query).await?;
        let stream = Box::pin(stream);

        Ok(stream)
    }

    fn get_field_schema_from_relation(&self, columns: &Vec<RelationColumn>) -> Vec<Field> {
        columns
            .iter()
            .map(|c| {
                let pg_col_type = self.pg_oid_type_map.get(&c.column_type);
                let unknown = String::from("unknown");
                let col_type = self
                    .pg_kafka_connect_type_map
                    .get(pg_col_type.unwrap_or(&unknown))
                    .unwrap_or(unknown);
                Field {
                    r#type: col_type,
                    optional: !c.is_pk,
                    field: c.column_name.clone(),
                    fields: None,
                }
            })
            .collect::<Vec<Field>>()
    }

    fn get_field_values(&self, fields: &Vec<Field>, columns: &Vec<ColumnData>) -> Value {
        let mut values: Value = json!({});

        for (f, c) in fields.iter().zip(columns.iter()) {
            let field_name = f.field.clone();
            let field_value = c.column_value.clone();
            values[field_name] = json!(field_value);
        }

        values
    }

    fn get_relation_event(&self, relation_id: u32) -> DMSRResult<&RelationEvent> {
        self.relation_map
            .get(&relation_id)
            .ok_or(DMSRError::PostgresError("Relation not found".into()))
    }

    fn get_default_fields(&self, db_fields: &Vec<Field>) -> Vec<Field> {
        let before = Field {
            r#type: "struct".into(),
            optional: true,
            field: "before".into(),
            fields: Some(db_fields.clone()),
        };

        let after = Field {
            r#type: "struct".into(),
            optional: true,
            field: "after".into(),
            fields: Some(db_fields.clone()),
        };

        let op = Field {
            r#type: "string".into(),
            optional: false,
            field: "op".into(),
            fields: None,
        };

        let ts_ms = Field {
            r#type: "int64".into(),
            optional: true,
            field: "ts_ms".into(),
            fields: None,
        };

        vec![before, after, op, ts_ms]
    }

    fn get_schema(&self, relation_event: &RelationEvent, default_fields: &[Field]) -> Schema {
        let schema_name = format!(
            "{}.{}",
            &relation_event.namespace, &relation_event.relation_name
        );

        Schema {
            r#type: "struct".to_string(),
            fields: default_fields.to_vec(),
            optional: false,
            name: schema_name,
        }
    }

    async fn process_insert_event(&self, event: InsertEvent, kafka: &Kafka) -> DMSRResult<()> {
        let relation_id = event.relation_id;
        let relation_event = self.get_relation_event(relation_id)?;

        let db_fields = self.get_field_schema_from_relation(&relation_event.columns);
        let db_values = self.get_field_values(&db_fields, &event.columns);

        let default_fields = self.get_default_fields(&db_fields);

        let schema = self.get_schema(relation_event, &default_fields);

        let payload = Payload {
            before: None,
            after: Some(db_values),
            op: "c".to_string(),
            ts_ms: event.timestamp.timestamp_millis(),
        };

        let kafka_message = KafkaMessage { schema, payload };

        kafka
            .ingest("test-topic".to_string(), &kafka_message, None)
            .await?;

        println!("{:?}", kafka_message);

        Ok(())
    }
}
