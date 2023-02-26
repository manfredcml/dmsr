use crate::streamers::streamer_config::StreamerConfig;
use async_trait::async_trait;

#[async_trait]
pub trait Streamer {
    fn new(config: &StreamerConfig) -> anyhow::Result<Box<Self>>
    where
        Self: Sized;
    async fn connect(&mut self) -> anyhow::Result<()>;
    async fn ingest(&mut self, data: Vec<u8>) -> anyhow::Result<()>;
}
