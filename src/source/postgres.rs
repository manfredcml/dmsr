use crate::source::base::Source;
use postgres::{Client, NoTls};
use anyhow::Result;

struct PostgresSource {
  endpoint: String,
  client: Option<Client>,
}

impl Source for PostgresSource {
  fn new(endpoint: String) -> Self {
    PostgresSource { endpoint, client: None }
  }

  fn get_endpoint(&self) -> &String {
    &self.endpoint
  }

  fn connect(&mut self) -> Result<()> {
    // Connect to the database and assign to self.client
    let client = Client::connect("", NoTls)?;
    self.client = Some(client);
    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn it_works() {
    // let result = add(2, 2);
    assert_eq!(4, 4);
  }
}