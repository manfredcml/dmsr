use dms::sources_targets::source_config::SourceConfig;
use dms::queue::config::QueueConfig;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub struct AppConfig {
    pub sources: Vec<SourceConfig>,
    pub queue: QueueConfig,
}
