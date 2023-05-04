pub trait ConnectorMetadata: serde::Serialize {
    fn kafka_topic(&self) -> String;
    fn schema(&self) -> &str;
    fn table(&self) -> &str;
    fn connector_name(&self) -> &str;
}
