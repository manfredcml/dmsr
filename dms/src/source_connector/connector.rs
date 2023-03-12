use crate::kafka::kafka_impl::Kafka;
use async_trait::async_trait;
use futures::lock::Mutex;
use std::sync::Arc;

#[async_trait]
pub trait SourceConnector {
    type Config;

    fn new(config: &Self::Config) -> anyhow::Result<Box<Self>>
    where
        Self: Sized;
    fn get_source_name(&self) -> anyhow::Result<String>;
    async fn connect(&mut self) -> anyhow::Result<()>;
    async fn stream(&mut self, queue: Arc<Mutex<Kafka>>) -> anyhow::Result<()>;
}
