use crate::queue::queue::Queue;
use crate::sources::config::SourceConfig;
use async_trait::async_trait;
use futures::lock::Mutex;
use std::sync::Arc;

#[async_trait]
pub trait Source {
    fn new(config: &SourceConfig) -> anyhow::Result<Box<Self>>
    where
        Self: Sized;
    async fn connect(&mut self) -> anyhow::Result<()>;
    async fn stream(&mut self, queue: Arc<Mutex<Box<dyn Queue + Send>>>) -> anyhow::Result<()>;
}
