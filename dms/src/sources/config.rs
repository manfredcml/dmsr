use crate::sources::postgres::PostgresSource;
use crate::sources::source::Source;
use crate::sources::source_type::SourceType;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub struct SourceConfig {
    pub name: String,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    #[serde(deserialize_with = "deserialize_source_type")]
    pub source_type: SourceType,
}

impl SourceConfig {
    pub fn get_source(self) -> Option<Box<dyn Source>> {
        match self.source_type {
            SourceType::Postgres => Some(Box::new(PostgresSource::new(self))),
            _ => None,
        }
    }
}

fn deserialize_source_type<'de, D>(deserializer: D) -> Result<SourceType, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    match s.as_str() {
        "postgres" => Ok(SourceType::Postgres),
        _ => Err(serde::de::Error::custom(format!(
            "Unknown source type: {}",
            s
        ))),
    }
}
