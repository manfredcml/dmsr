use crate::error::error::DMSRResult;
use crate::kafka::kafka::Kafka;
use async_trait::async_trait;

#[async_trait]
pub trait Connector {
    type Config;

    async fn new(connector_name: String, config: &Self::Config) -> DMSRResult<Box<Self>>
    where
        Self: Sized;

    async fn stream(&mut self, kafka: Kafka) -> DMSRResult<()>;
}
