use crate::queue::queue_config::QueueConfig;
use async_trait::async_trait;
use crate::events::standardized_event::Event;

#[async_trait]
pub trait Queue {
    fn new(config: &QueueConfig) -> anyhow::Result<Box<Self>>
    where
        Self: Sized;
    async fn connect(&mut self) -> anyhow::Result<()>;
    async fn ingest(&mut self, event: Event) -> anyhow::Result<()>;
}
