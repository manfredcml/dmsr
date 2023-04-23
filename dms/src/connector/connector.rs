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

    async fn new(connector_name: String, config: Self::Config) -> DMSRResult<Box<Self>>
    where
        Self: Sized;

    async fn make_kafka_message_stream(&mut self) -> DMSRResult<KafkaMessageStream>;

    async fn send_to_kafka(&self, kafka: &Kafka, stream: &mut KafkaMessageStream) -> DMSRResult<()>;
}
