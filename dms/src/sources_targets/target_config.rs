use crate::sources_targets::postgres_config::PostgresConfig;
use crate::sources_targets::target_kind::TargetKind;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TargetConfig {
    pub name: String,
    pub kind: TargetKind,
    #[serde(rename = "postgres")]
    pub postgres_config: Option<PostgresConfig>,
}
