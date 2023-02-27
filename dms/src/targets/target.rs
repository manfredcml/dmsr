use crate::events::event::ChangeEvent;
use crate::targets::config::TargetConfig;
use async_trait::async_trait;

#[async_trait]
pub trait Target {
    fn new(config: &TargetConfig) -> anyhow::Result<Box<Self>>
    where
        Self: Sized;
    async fn connect(&mut self) -> anyhow::Result<()>;
    async fn accept(&mut self, data: ChangeEvent) -> anyhow::Result<()>;
}
