use crate::error::DMSRResult;
use crate::kafka::kafka_client::Kafka;
use async_trait::async_trait;

#[async_trait]
pub trait SourceConnector {
    type Config;

    async fn new(name: String, config: Self::Config) -> DMSRResult<Box<Self>>
    where
        Self: Sized;

    async fn snapshot(&mut self, kafka: &Kafka) -> DMSRResult<()>;

    async fn stream(&mut self, kafka: &Kafka) -> DMSRResult<()>;
}
