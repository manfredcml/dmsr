use crate::source::base::Source;
use postgres::{Client, NoTls, Row};
use anyhow::{Result, anyhow};

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
    println!("Connecting to {}", self.endpoint.as_str());
    let client = Client::connect(
      self.endpoint.as_str(),
      NoTls,
    )?;
    self.client = Some(client);
    Ok(())
  }

  fn query<Row>(&mut self, query: String) -> Result<Vec<Row>> {
    let client = match self.client.as_mut() {
      Some(client) => client,
      None => return Err(anyhow!("Failed to connect to Postgres")),
    };
    for row in client.query(query.as_str(), &[]) {
      println!("Row: {:?}", row);
    }
    return Ok(vec![]);
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn it_works() {
    let conn_str = "host=localhost user=postgres password=kbP4ZyAyEQ";
    let mut source = PostgresSource::new(conn_str.to_string());
    source.connect().expect("Failed to connect to Postgres");
    let _: Vec<Row> = source
      .query("SELECT * FROM public.test_tbl".to_string())
      .expect("Failed to query Postgres");
  }
}