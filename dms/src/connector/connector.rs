use crate::kafka::kafka::Kafka;
use async_trait::async_trait;
use futures::lock::Mutex;
use std::sync::Arc;

#[async_trait]
pub trait Connector {
    type Config;

    fn new(config: &Self::Config) -> anyhow::Result<Box<Self>>
    where
        Self: Sized;

    async fn connect(&mut self) -> anyhow::Result<()>;
    async fn stream(&mut self, kafka: Arc<Mutex<Kafka>>) -> anyhow::Result<()>;
}
