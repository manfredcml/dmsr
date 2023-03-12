use crate::event::event::ChangeEvent;
use async_trait::async_trait;

#[async_trait]
pub trait SinkConnector {
    type Config;

    fn new(config: &Self::Config) -> anyhow::Result<Box<Self>>
    where
        Self: Sized;
    fn get_name(&self) -> anyhow::Result<&String>;
    async fn connect(&mut self) -> anyhow::Result<()>;
    async fn accept(&mut self, data: ChangeEvent) -> anyhow::Result<()>;
}
