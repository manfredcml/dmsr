use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct KafkaConfig {
    pub bootstrap_servers: String,
}
