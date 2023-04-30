pub trait ConnectorMetadata: serde::Serialize {
    fn get_kafka_topic(&self) -> String;
    fn get_schema(&self) -> &str;
    fn get_table(&self) -> &str;
}
