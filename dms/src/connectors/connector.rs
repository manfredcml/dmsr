use crate::connectors::config::ConnectorConfig;
use crate::queue::queue::Queue;
use crate::sources_targets::target::Target;
use async_trait::async_trait;
use futures::lock::Mutex;
use std::sync::Arc;

#[async_trait]
pub trait Connector {
    fn new(config: &ConnectorConfig) -> anyhow::Result<Box<Self>>
    where
        Self: Sized;
    fn get_name(&self) -> &String;
    fn get_config(&self) -> &ConnectorConfig;
    async fn stitch(
        &mut self,
        queue: Arc<Mutex<Box<dyn Queue + Send>>>,
        target: Arc<Mutex<Box<dyn Target + Send>>>,
    ) -> anyhow::Result<()>;
}
