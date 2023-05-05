use crate::error::error::DMSRResult;
use crate::kafka::kafka::Kafka;
use crate::kafka::message::KafkaMessage;
use async_trait::async_trait;
use futures::Stream;
use std::pin::Pin;

pub type KafkaMessageStream = Pin<Box<dyn Stream<Item = DMSRResult<KafkaMessage>> + Send + Sync>>;

#[async_trait]
pub trait SourceConnector {
    type Config;
    type Metadata;

    async fn new(name: String, config: Self::Config) -> DMSRResult<Box<Self>>
    where
        Self: Sized;

    async fn snapshot(&mut self, kafka: &Kafka) -> DMSRResult<()>;

    async fn stream_messages(&mut self, kafka: &Kafka) -> DMSRResult<KafkaMessageStream>;

    async fn publish_messages(
        &self,
        kafka: &Kafka,
        stream: &mut KafkaMessageStream,
    ) -> DMSRResult<()>;
}
