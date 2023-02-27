use crate::events::event::ChangeEvent;
use crate::targets::config::TargetConfig;
use crate::targets::kind::TargetKind;
use crate::targets::postgres_config::PostgresConfig;
use crate::targets::target::Target;
use anyhow::anyhow;
use async_trait::async_trait;
use tokio_postgres::{Client, NoTls};

#[derive(Debug)]
pub struct PostgresTarget {
    config: TargetConfig,
    client: Option<Client>,
}

impl PostgresTarget {
    fn get_postgres_config(&self) -> anyhow::Result<&PostgresConfig> {
        return match &self.config.postgres_config {
            Some(config) => Ok(config),
            None => Err(anyhow!("Invalid config for PostgresTarget")),
        };
    }
}

#[async_trait]
impl Target for PostgresTarget {
    fn new(config: &TargetConfig) -> anyhow::Result<Box<Self>> {
        if config.kind != TargetKind::Postgres {
            return Err(anyhow!("Invalid source type for PostgresSource"));
        }

        if config.postgres_config == None {
            return Err(anyhow!("Invalid config for PostgresSource"));
        }

        Ok(Box::new(PostgresTarget {
            config: config.clone(),
            client: None,
        }))
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        let config = self.get_postgres_config()?;

        let endpoint = format!(
            "host={} port={} user={} password={} replication=database",
            config.host, config.port, config.user, config.password
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
        // let client = self.client.as_mut().unwrap();
        // let postgres_event = data.postgres_event.as_ref().unwrap();
        // let query = postgres_event.query.as_str();
        // let params = &postgres_event.params;
        // let result = client.execute(query, params).await?;
        // println!("result: {}", result);
        Ok(())
    }
}
