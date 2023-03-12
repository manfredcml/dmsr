use crate::event::event::ChangeEvent;
use async_trait::async_trait;

#[async_trait]
pub trait Queue {
    type Config;

    fn new(config: &Self::Config) -> anyhow::Result<Box<Self>>
    where
        Self: Sized;
    async fn connect(&mut self) -> anyhow::Result<()>;
    async fn ingest(&mut self, event: ChangeEvent) -> anyhow::Result<()>;
}
