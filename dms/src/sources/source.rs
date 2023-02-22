use anyhow::Result;
use crate::sources::config::Config;
use async_trait::async_trait;
use tokio::sync::mpsc::Sender;
use crate::events::standardized_event::Event;

#[async_trait]
pub trait Source {
  type QueryRow;
  fn new(config: Config) -> Self;
  fn get_config(&self) -> &Config;
  async fn connect(&mut self) -> Result<()>;
  async fn stream(&mut self, tx: &mut Sender<Event>) -> Result<()>;
}
