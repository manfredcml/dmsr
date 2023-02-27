use crate::queue::queue::Queue;
use crate::sources_targets::source_config::SourceConfig;
use async_trait::async_trait;
use futures::lock::Mutex;
use std::sync::Arc;

#[async_trait]
pub trait Source {
    fn new(config: &SourceConfig) -> anyhow::Result<Box<Self>>
    where
        Self: Sized;
    fn get_source_name(&self) -> anyhow::Result<&String>;
    fn get_source_config(&self) -> anyhow::Result<&SourceConfig>;
    async fn connect(&mut self) -> anyhow::Result<()>;
    async fn stream(&mut self, queue: Arc<Mutex<Box<dyn Queue + Send>>>) -> anyhow::Result<()>;
}
