use anyhow::Result;
use crate::sources::config::Config;
use async_trait::async_trait;

#[async_trait]
pub trait Source {
  type QueryRow;

  fn new(config: Config) -> Self;
  fn get_config(&self) -> &Config;

  async fn connect(&mut self) -> Result<()>;
  async fn query(&mut self, query: String) -> Result<Vec<Self::QueryRow>>;
  async fn stream(&mut self) -> Result<()>;
}
