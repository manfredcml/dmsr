use crate::error::missing_value::MissingValueError;
use rdkafka::error::KafkaError;

pub type DMSRResult<T> = Result<T, DMSRError>;

pub enum DMSRError {
    MissingValueError(MissingValueError),
    KafkaError(KafkaError),
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
