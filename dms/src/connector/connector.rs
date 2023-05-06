use crate::error::DMSRResult;
use crate::kafka::kafka::Kafka;
use crate::kafka::message::KafkaMessage;
use async_trait::async_trait;
use futures::Stream;
use std::pin::Pin;

#[async_trait]
pub trait SourceConnector {
    type Config;

    async fn new(name: String, config: Self::Config) -> DMSRResult<Box<Self>>
    where
        Self: Sized;

    async fn snapshot(&mut self, kafka: &Kafka) -> DMSRResult<()>;

    async fn stream(&mut self, kafka: &Kafka) -> DMSRResult<()>;
}
