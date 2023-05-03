use clap::Parser;
use dms::kafka::config::KafkaConfig;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(default_value = "8000")]
    pub port: u16,

    #[arg(default_value = "localhost:9092")]
    pub bootstrap_servers: String,

    #[arg(default_value = "dmsr-configs")]
    pub config_topic: String,

    #[arg(default_value = "dmsr-offsets")]
    pub offset_topic: String,

    #[arg(default_value = "dmsr-status")]
    pub status_topic: String,
}

impl Args {
    pub fn to_kafka_config(&self) -> KafkaConfig {
        KafkaConfig {
            bootstrap_servers: self.bootstrap_servers.clone(),
            config_topic: self.config_topic.clone(),
            offset_topic: self.offset_topic.clone(),
            status_topic: self.status_topic.clone(),
        }
    }
}
