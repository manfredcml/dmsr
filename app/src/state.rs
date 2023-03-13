use crate::config::AppConfig;
use dms::kafka::kafka::Kafka;

pub struct AppState {
    pub kafka: Kafka,
    pub app_config: AppConfig,
}
