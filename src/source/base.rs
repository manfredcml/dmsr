use anyhow::Result;

pub trait Source {
  fn new(endpoint: String) -> Self;
  fn get_endpoint(&self) -> &String;
  fn connect(&mut self) -> Result<()>;
}
