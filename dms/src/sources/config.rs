use crate::sources::postgres::PostgresSource;
use crate::sources::postgres_config::PostgresConfig;
use crate::sources::source::Source;
use crate::sources::source_kind::SourceKind;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct SourceConfig {
    pub kind: SourceKind,
    pub name: String,
    #[serde(rename = "postgres")]
    pub postgres_config: Option<PostgresConfig>,
}

impl SourceConfig {
    pub fn get_source(self) -> anyhow::Result<Box<dyn Source + Send>> {
        match self.kind {
            SourceKind::Postgres => {
                let postgres = PostgresSource::new(&self)?;
                Ok(postgres)
            }
        }
    }
}
