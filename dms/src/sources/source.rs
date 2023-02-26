use crate::events::standardized_event::Event;
use crate::sources::config::SourceConfig;
use async_trait::async_trait;
use tokio::sync::mpsc::Sender;

#[async_trait]
pub trait Source {
    fn new(config: SourceConfig) -> Self
    where
        Self: Sized;
    fn get_config(&self) -> &SourceConfig;
    async fn connect(&mut self) -> anyhow::Result<()>;
    async fn stream(&mut self, tx: &mut Sender<Event>) -> anyhow::Result<()>;
}
