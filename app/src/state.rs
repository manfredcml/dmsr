use crate::config::AppConfig;
use dms::kafka::kafka_impl::Kafka;

pub struct AppState {
    pub kafka: Kafka,
    pub app_config: AppConfig,
}
