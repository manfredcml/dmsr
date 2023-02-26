use crate::sources::postgres::PostgresSource;
use crate::sources::postgres_config::PostgresConfig;
use crate::sources::source::Source;
use crate::sources::source_kind::SourceKind;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct SourceConfig {
    #[serde(deserialize_with = "deserialize_source_type")]
    pub kind: SourceKind,
    pub name: String,
    pub postgres_config: Option<PostgresConfig>,
}

impl SourceConfig {
    pub fn get_source(self) -> anyhow::Result<Box<dyn Source + Send>> {
        match self.kind {
            SourceKind::Postgres => {
                let postgres = PostgresSource::new(&self)?;
                Ok(postgres)
            },
        }
    }
}

fn deserialize_source_type<'de, D>(deserializer: D) -> Result<SourceKind, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    match s.as_str() {
        "postgres" => Ok(SourceKind::Postgres),
        _ => Err(serde::de::Error::custom(format!(
            "Unknown source type: {}",
            s
        ))),
    }
}
