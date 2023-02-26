use dms::sources::config::SourceConfig;
use dms::queue::queue_config::QueueConfig;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub struct AppConfig {
    pub sources: Vec<SourceConfig>,
    pub queue: QueueConfig,
}
