use dms::sources::source_type::SourceType;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub struct AppConfig {
    pub source_configs: Vec<SourceConfig>,
}

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
