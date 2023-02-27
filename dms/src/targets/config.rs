use crate::targets::kind::TargetKind;
use crate::targets::postgres_config::PostgresConfig;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TargetConfig {
    pub name: String,
    pub kind: TargetKind,
    #[serde(rename = "postgres")]
    pub postgres_config: Option<PostgresConfig>,
}
