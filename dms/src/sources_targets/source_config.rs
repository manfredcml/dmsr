use crate::sources_targets::postgres::Postgres;
use crate::sources_targets::postgres_config::PostgresConfig;
use crate::sources_targets::source::Source;
use crate::sources_targets::source_kind::SourceKind;
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
                let postgres = Postgres::new(&self)?;
                Ok(postgres)
            }
        }
    }
}
