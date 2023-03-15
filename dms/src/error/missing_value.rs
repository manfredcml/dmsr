use std::error::Error;
use std::fmt::{self, Display};

#[derive(Debug)]
pub struct MissingValueError {
    pub field_name: &'static str,
}

impl MissingValueError {
    pub fn new(field_name: &'static str) -> Self {
        MissingValueError { field_name }
    }
}

impl Error for MissingValueError {}

impl Display for MissingValueError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Missing value for field '{}'", self.field_name)
    }
}
