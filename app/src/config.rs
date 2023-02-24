use dms::sources::config::SourceConfig;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub struct AppConfig {
    pub sources: Vec<SourceConfig>,
}
