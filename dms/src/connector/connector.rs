use crate::error::generic::DMSRResult;
use crate::kafka::kafka::Kafka;
use async_trait::async_trait;

#[async_trait]
pub trait Connector {
    type Config;

    fn new(config: &Self::Config) -> DMSRResult<Box<Self>>
    where
        Self: Sized;

    async fn connect(&mut self) -> DMSRResult<()>;
    async fn stream(&mut self, kafka: Kafka) -> DMSRResult<()>;
}
