use crate::events::event::ChangeEvent;
use crate::sources_targets::target_config::TargetConfig;
use async_trait::async_trait;

#[async_trait]
pub trait Target {
    fn new(config: &TargetConfig) -> anyhow::Result<Box<Self>>
    where
        Self: Sized;
    fn get_target_name(&self) -> anyhow::Result<&String>;
    fn get_target_config(&self) -> anyhow::Result<&TargetConfig>;
    async fn connect(&mut self) -> anyhow::Result<()>;
    async fn accept(&mut self, data: ChangeEvent) -> anyhow::Result<()>;
}
