use std::error::Error;
use std::fmt::{self, Display};

#[derive(Debug)]
pub struct UnknownConnectorError {
    pub connector: String,
}

impl Error for UnknownConnectorError {}

impl Display for UnknownConnectorError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Unknown connector '{}'", self.connector)
    }
}
