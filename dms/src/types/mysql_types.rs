use crate::error::error::{DMSRError, DMSRResult};
use std::collections::HashMap;
use std::str::FromStr;
use bytes::Buf;

#[derive(Debug, PartialEq)]
pub struct MySQLTypeMap {
    kafka_connect_map: HashMap<&'static str, &'static str>,
    num_byte_map: HashMap<&'static str, usize>,
}

impl Default for MySQLTypeMap {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, PartialEq)]
pub enum MySQLType {
    TINYINT,
    SMALLINT,
    MEDIUMINT,
    INT,
    BIGINT,
    FLOAT,
    DOUBLE,
    DECIMAL,
    BIT,
}

impl MySQLType {
    pub fn get_size_in_bytes(&self, col_metadata: Option<&[u8]>) -> DMSRResult<usize> {
        let mut col_metadata = col_metadata.unwrap_or_default();
        let size: usize = match self {
            MySQLType::TINYINT => 1,
            MySQLType::SMALLINT => 2,
            MySQLType::MEDIUMINT => 3,
            MySQLType::INT => 4,
            MySQLType::BIGINT => 8,
            MySQLType::FLOAT => col_metadata.get_u8() as usize,
            MySQLType::DOUBLE => 8,
            MySQLType::DECIMAL => 8,
            MySQLType::BIT => 8,
        };
        Ok(size)
    }
}

impl FromStr for MySQLType {
    type Err = DMSRError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "tinyint" => Ok(MySQLType::TINYINT),
            "smallint" => Ok(MySQLType::SMALLINT),
            "mediumint" => Ok(MySQLType::MEDIUMINT),
            "int" => Ok(MySQLType::INT),
            "bigint" => Ok(MySQLType::BIGINT),
            "float" => Ok(MySQLType::FLOAT),
            "double" => Ok(MySQLType::DOUBLE),
            "decimal" => Ok(MySQLType::DECIMAL),
            "bit" => Ok(MySQLType::BIT),
            _ => Err(DMSRError::MySQLError(format!("Unknown MySQL type: {}", s))),
        }
    }
}

impl MySQLTypeMap {
    pub fn new() -> Self {
        let kafka_connect_map = Self::get_kafka_connect_map();
        let num_byte_map = Self::get_num_byte_map();
        MySQLTypeMap {
            kafka_connect_map,
            num_byte_map,
        }
    }

    fn get_num_byte_map() -> HashMap<&'static str, usize> {
        let mut map = HashMap::new();
        map.insert("tinyint", 1);
        map.insert("smallint", 2);
        map.insert("mediumint", 3);
        map.insert("int", 4);
        map.insert("bigint", 8);
        map.insert("float", 4);
        map.insert("double", 8);
        map.insert("date", 3);
        map.insert("datetime", 8);
        map
    }

    fn get_kafka_connect_map() -> HashMap<&'static str, &'static str> {
        let mut map: HashMap<&'static str, &'static str> = HashMap::new();
        map.insert("tinyint", "int8");
        map.insert("smallint", "int16");
        map.insert("mediumint", "int32");
        map.insert("int", "int32");
        map.insert("bigint", "int64");
        map.insert("float", "float32");
        map.insert("double", "float64");
        map.insert("decimal", "decimal");
        map.insert("numeric", "decimal");
        map.insert("date", "date");
        map.insert("datetime", "timestamp");
        map.insert("timestamp", "timestamp");
        map.insert("time", "int64");
        map.insert("year", "int32");
        map.insert("char", "string");
        map.insert("varchar", "string");
        map.insert("binary", "bytes");
        map.insert("varbinary", "bytes");
        map.insert("tinyblob", "bytes");
        map.insert("blob", "bytes");
        map.insert("mediumblob", "bytes");
        map.insert("longblob", "bytes");
        map.insert("tinytext", "string");
        map.insert("text", "string");
        map.insert("mediumtext", "string");
        map.insert("longtext", "string");
        map.insert("enum", "string");
        map.insert("set", "string");
        map.insert("bit", "bytes");
        map.insert("geometry", "bytes");
        map
    }

    pub fn get(&self, key: &str) -> DMSRResult<String> {
        self.kafka_connect_map
            .get(key)
            .map(|v| v.to_string())
            .ok_or(DMSRError::MySQLError(format!(
                "No Kafka connect type found for MySQL type: {}",
                key
            )))
    }
}
