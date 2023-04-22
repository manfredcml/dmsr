// use crate::connector::connector::SourceConnector;
// use crate::connector::postgres_sink::config::PostgresSinkConfig;
// use crate::error::error::{DMSRError, DMSRResult};
// use crate::kafka::kafka::Kafka;
// use async_trait::async_trait;
// use rdkafka::consumer::Consumer;
// use rdkafka::Message;
// use std::collections::HashSet;
// use tokio_postgres::NoTls;
//
// pub struct PostgresSinkConnector {
//     config: PostgresSinkConfig,
//     client: Option<tokio_postgres::Client>,
//     connector_name: String,
// }
//
// #[async_trait]
// impl SourceConnector for PostgresSinkConnector {
//     type Config = PostgresSinkConfig;
//
//     async fn new(connector_name: String, config: &PostgresSinkConfig) -> DMSRResult<Box<Self>> {
//         Ok(Box::new(PostgresSinkConnector {
//             config: config.clone(),
//             client: None,
//             connector_name,
//         }))
//     }
//
//     // async fn connect(&mut self) -> DMSRResult<()> {
//     //     let endpoint = format!(
//     //         "host={} port={} user={} password={}",
//     //         self.config.host, self.config.port, self.config.user, self.config.password
//     //     );
//     //
//     //     let (client, connection) = tokio_postgres::connect(endpoint.as_str(), NoTls).await?;
//     //
//     //     tokio::spawn(async move {
//     //         if let Err(e) = connection.await {
//     //             eprintln!("connection error: {}", e);
//     //         }
//     //     });
//     //
//     //     self.client = Some(client);
//     //     Ok(())
//     // }
//
//     async fn cdc_events_to_stream(&mut self, kafka: &Kafka) -> DMSRResult<()> {
//         Ok(())
//         // println!("Starting stream for connector: {}", self.connector_name);
//         // let consumer = match &mut kafka.consumer {
//         //     Some(consumer) => consumer,
//         //     None => {
//         //         return Err(DMSRError::MissingValueError(
//         //             "Kafka consumer not initialized".to_string(),
//         //         ))
//         //     }
//         // };
//         // let topics_to_subscribe: Vec<String> = self
//         //     .config
//         //     .tables
//         //     .iter()
//         //     .map(|table| format!("{}-{}", self.topic_prefix, table))
//         //     .collect();
//         // let topics_to_subscribe: Vec<&str> =
//         //     topics_to_subscribe.iter().map(|s| s.as_str()).collect();
//         // let topics_to_subscribe = topics_to_subscribe.as_slice();
//         // consumer.subscribe(topics_to_subscribe)?;
//         //
//         // loop {
//         //     match consumer.recv().await {
//         //         Ok(msg) => {
//         //             let payload = msg.payload().unwrap_or_default();
//         //             let payload = String::from_utf8(payload.to_vec())?;
//         //             let payload: JSONChangeEvent = serde_json::from_str(&payload)?;
//         //
//         //             match payload.op {
//         //                 Operation::Create => {
//         //                     self.insert(payload).await?;
//         //                 }
//         //                 Operation::Update => {
//         //                     self.update(payload).await?;
//         //                 }
//         //                 Operation::Delete => {
//         //                     self.delete(payload).await?;
//         //                 }
//         //             }
//         //         }
//         //         Err(e) => {
//         //             eprintln!("Error while receiving message: {:?}", e);
//         //         }
//         //     }
//         // }
//     }
// }
//
// // impl PostgresSinkConnector {
// //     async fn insert(&mut self, event: JSONChangeEvent) -> DMSRResult<()> {
// //         let client = match self.client.as_mut() {
// //             Some(client) => client,
// //             None => {
// //                 return Err(DMSRError::MissingValueError(
// //                     "Client is not initialized".into(),
// //                 ))
// //             }
// //         };
// //
// //         let table = event.table;
// //
// //         let columns: Vec<String> = event
// //             .schema
// //             .fields
// //             .iter()
// //             .map(|f| f.field.clone())
// //             .collect();
// //
// //         let values: Vec<String> = columns
// //             .iter()
// //             .filter_map(|c| event.payload.get(c))
// //             .map(|v| v.to_string().replace('"', "'"))
// //             .collect();
// //
// //         let columns = columns.join(",");
// //         let values = values.join(",");
// //
// //         let query = format!("INSERT INTO {} ({}) VALUES ({})", table, columns, values);
// //         println!("{}", query);
// //
// //         client.execute(query.as_str(), &[]).await?;
// //
// //         Ok(())
// //     }
// //
// //     async fn update(&mut self, event: JSONChangeEvent) -> DMSRResult<()> {
// //         let client = match self.client.as_mut() {
// //             Some(client) => client,
// //             None => {
// //                 return Err(DMSRError::MissingValueError(
// //                     "Client is not initialized".into(),
// //                 ))
// //             }
// //         };
// //
// //         let table = event.table;
// //
// //         let columns: Vec<String> = event
// //             .schema
// //             .fields
// //             .iter()
// //             .map(|f| f.field.clone())
// //             .collect();
// //
// //         let values: Vec<String> = columns
// //             .iter()
// //             .filter_map(|c| event.payload.get(c))
// //             .map(|v| v.to_string().replace('"', "'"))
// //             .collect();
// //
// //         let mut query = format!("UPDATE {} SET ", table);
// //         query += &columns
// //             .iter()
// //             .zip(values.iter())
// //             .map(|(c, v)| format!("{} = {}", c, v))
// //             .collect::<Vec<String>>()
// //             .join(",");
// //
// //         query += " WHERE ";
// //
// //         let pk_set: HashSet<&String> = event.pk.iter().collect();
// //         let pk_value = event
// //             .payload
// //             .iter()
// //             .filter(|(k, _)| pk_set.contains(k))
// //             .map(|(k, v)| format!("{} = '{}'", k, v))
// //             .collect::<Vec<String>>()
// //             .join(" AND ");
// //
// //         query += &pk_value;
// //
// //         println!("{}", query);
// //
// //         client.execute(query.as_str(), &[]).await?;
// //
// //         Ok(())
// //     }
// //
// //     async fn delete(&mut self, event: JSONChangeEvent) -> DMSRResult<()> {
// //         let client = match self.client.as_mut() {
// //             Some(client) => client,
// //             None => {
// //                 return Err(DMSRError::MissingValueError(
// //                     "Client is not initialized".into(),
// //                 ))
// //             }
// //         };
// //
// //         let table = event.table;
// //
// //         let mut query = format!("DELETE FROM {}", table);
// //         query += " WHERE ";
// //
// //         let pk_set: HashSet<&String> = event.pk.iter().collect();
// //         let pk_value = event
// //             .payload
// //             .iter()
// //             .filter(|(k, _)| pk_set.contains(k))
// //             .map(|(k, v)| format!("{} = '{}'", k, v))
// //             .collect::<Vec<String>>()
// //             .join(" AND ");
// //
// //         query += &pk_value;
// //
// //         println!("{}", query);
// //
// //         client.execute(query.as_str(), &[]).await?;
// //
// //         Ok(())
// //     }
// // }
