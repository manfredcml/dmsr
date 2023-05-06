use dms::error::DMSRResult;
use dms::kafka::kafka::Kafka;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;

pub type ActiveConnectors = HashMap<String, JoinHandle<DMSRResult<()>>>;
pub type MutexActiveConnectors = Arc<Mutex<ActiveConnectors>>;

pub struct AppState {
    pub kafka: Kafka,
    pub active_connectors: MutexActiveConnectors,
}
