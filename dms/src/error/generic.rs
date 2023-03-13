use crate::error::missing_value::MissingValueError;
use rdkafka::error::KafkaError;

pub type DMSRResult<T> = Result<T, DMSRError>;

#[derive(Debug)]
pub enum DMSRError {
    MissingValueError(MissingValueError),
    KafkaError(KafkaError),
    StdIoError(std::io::Error),
    SerdeYamlError(serde_yaml::Error),
}

impl From<std::io::Error> for DMSRError {
    fn from(error: std::io::Error) -> Self {
        DMSRError::StdIoError(error)
    }
}

impl From<KafkaError> for DMSRError {
    fn from(error: KafkaError) -> Self {
        DMSRError::KafkaError(error)
    }
}

impl From<MissingValueError> for DMSRError {
    fn from(error: MissingValueError) -> Self {
        DMSRError::MissingValueError(error)
    }
}

impl From<serde_yaml::Error> for DMSRError {
    fn from(error: serde_yaml::Error) -> Self {
        DMSRError::SerdeYamlError(error)
    }
}
