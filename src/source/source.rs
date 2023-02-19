use anyhow::Result;
use crate::source::config::Config;
use async_trait::async_trait;

#[async_trait]
pub trait Source {
  type Output;

  fn new(config: Config) -> Self;
  fn get_endpoint(&self) -> &String;
  async fn connect(&mut self) -> Result<()>;
  async fn query(&mut self, query: String) -> Result<Vec<Self::Output>>;
}
