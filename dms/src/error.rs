use rdkafka::error::KafkaError;
use std::time;

pub type DMSRResult<T> = Result<T, DMSRError>;

#[derive(Debug)]
pub enum DMSRError {
    MissingValueError(String),
    KafkaError(String),
    StdIoError(std::io::Error),
    SerdeJsonError(serde_json::Error),
    Utf8Error(String),
    UnknownConnectorError(String),
    PostgresError(String),
    SystemTimeError(String),
    TryFromIntError(String),
    MySQLError(String),
    UnimplementedError(String),
    StrumParseError(String),
    LockError(String),
    ParseIntError(String),
    TokioJoinError(tokio::task::JoinError),
    RegexError(regex::Error),
    MySQLSourceConnectorError(Box<dyn std::error::Error + Send + Sync>),
}

impl From<sqlparser::parser::ParserError> for DMSRError {
    fn from(error: sqlparser::parser::ParserError) -> Self {
        DMSRError::MySQLSourceConnectorError(Box::new(error))
    }
}

impl From<regex::Error> for DMSRError {
    fn from(error: regex::Error) -> Self {
        DMSRError::RegexError(error)
    }
}

impl From<tokio::task::JoinError> for DMSRError {
    fn from(error: tokio::task::JoinError) -> Self {
        DMSRError::TokioJoinError(error)
    }
}

impl From<mysql_async::binlog::events::BadColumnType> for DMSRError {
    fn from(error: mysql_async::binlog::events::BadColumnType) -> Self {
        DMSRError::MySQLError(error.to_string())
    }
}

impl From<mysql_async::binlog::UnknownEventType> for DMSRError {
    fn from(error: mysql_async::binlog::UnknownEventType) -> Self {
        DMSRError::MySQLError(error.to_string())
    }
}

impl From<std::num::ParseIntError> for DMSRError {
    fn from(error: std::num::ParseIntError) -> Self {
        DMSRError::ParseIntError(error.to_string())
    }
}

impl From<strum::ParseError> for DMSRError {
    fn from(error: strum::ParseError) -> Self {
        DMSRError::StrumParseError(error.to_string())
    }
}

impl From<mysql_async::Error> for DMSRError {
    fn from(error: mysql_async::Error) -> Self {
        DMSRError::MySQLError(error.to_string())
    }
}

impl From<time::SystemTimeError> for DMSRError {
    fn from(error: time::SystemTimeError) -> Self {
        DMSRError::SystemTimeError(error.to_string())
    }
}

impl From<std::num::TryFromIntError> for DMSRError {
    fn from(error: std::num::TryFromIntError) -> Self {
        DMSRError::TryFromIntError(error.to_string())
    }
}

impl From<tokio_postgres::Error> for DMSRError {
    fn from(error: tokio_postgres::Error) -> Self {
        DMSRError::PostgresError(error.to_string())
    }
}

impl From<serde_json::Error> for DMSRError {
    fn from(error: serde_json::Error) -> Self {
        DMSRError::SerdeJsonError(error)
    }
}

impl From<std::string::FromUtf8Error> for DMSRError {
    fn from(error: std::string::FromUtf8Error) -> Self {
        DMSRError::Utf8Error(error.to_string())
    }
}

impl From<std::str::Utf8Error> for DMSRError {
    fn from(error: std::str::Utf8Error) -> Self {
        DMSRError::Utf8Error(error.to_string())
    }
}

impl From<std::io::Error> for DMSRError {
    fn from(error: std::io::Error) -> Self {
        DMSRError::StdIoError(error)
    }
}

impl From<KafkaError> for DMSRError {
    fn from(error: KafkaError) -> Self {
        DMSRError::KafkaError(error.to_string())
    }
}
