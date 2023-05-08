use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub enum Operation {
    #[serde(rename = "c")]
    Create,
    #[serde(rename = "u")]
    Update,
    #[serde(rename = "d")]
    Delete,
    #[serde(rename = "t")]
    Truncate,
    #[serde(rename = "r")]
    Snapshot,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{from_str, to_string};

    #[test]
    fn test_operation_serialization() {
        assert_eq!(to_string(&Operation::Create).unwrap(), "\"c\"");
        assert_eq!(to_string(&Operation::Update).unwrap(), "\"u\"");
        assert_eq!(to_string(&Operation::Delete).unwrap(), "\"d\"");
        assert_eq!(to_string(&Operation::Truncate).unwrap(), "\"t\"");
        assert_eq!(to_string(&Operation::Snapshot).unwrap(), "\"r\"");
    }

    #[test]
    fn test_operation_deserialization() {
        assert_eq!(from_str::<Operation>("\"c\"").unwrap(), Operation::Create);
        assert_eq!(from_str::<Operation>("\"u\"").unwrap(), Operation::Update);
        assert_eq!(from_str::<Operation>("\"d\"").unwrap(), Operation::Delete);
        assert_eq!(from_str::<Operation>("\"t\"").unwrap(), Operation::Truncate);
        assert_eq!(from_str::<Operation>("\"r\"").unwrap(), Operation::Snapshot);
    }
}
