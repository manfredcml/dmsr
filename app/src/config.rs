use dms::sources::config::SourceConfig;
use dms::streamers::streamer_config::StreamerConfig;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub struct AppConfig {
    pub sources: Vec<SourceConfig>,
    pub streamer: StreamerConfig,
}
