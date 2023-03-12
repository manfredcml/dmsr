use crate::event::event::ChangeEvent;
use crate::sink_connector::connector::SinkConnector;
use crate::sink_connector::postgres_config::PostgresSinkConfig;
use async_trait::async_trait;
use tokio_postgres::{Client, NoTls};

pub struct PostgresSinkConnector {
    config: PostgresSinkConfig,
    client: Option<Client>,
}

#[async_trait]
impl SinkConnector for PostgresSinkConnector {
    type Config = PostgresSinkConfig;

    fn new(config: &PostgresSinkConfig) -> anyhow::Result<Box<Self>> {
        Ok(Box::new(PostgresSinkConnector {
            config: config.clone(),
            client: None,
        }))
    }

    fn get_name(&self) -> anyhow::Result<&String> {
        Ok(&"123".to_string())
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        let endpoint = format!(
            "host={} port={} user={} password={}",
            self.config.host, self.config.port, self.config.user, self.config.password
        );

        let (client, connection) = tokio_postgres::connect(endpoint.as_str(), NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        self.client = Some(client);
        Ok(())
    }

    async fn accept(&mut self, data: ChangeEvent) -> anyhow::Result<()> {
        println!("{:?}", data);
        Ok(())
    }
}
