use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub enum QueueKind {
    #[serde(rename = "kafka")]
    Kafka,
}
