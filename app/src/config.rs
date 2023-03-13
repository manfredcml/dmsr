use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::hash::Hash;

#[derive(Deserialize, Serialize, Debug)]
pub struct AppConfig {
    pub kafka: KafkaConfig,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct KafkaConfig {
    pub bootstrap_servers: String,
    pub config_topic: String,
}
