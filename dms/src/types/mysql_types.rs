use crate::error::error::{DMSRError, DMSRResult};
use std::collections::HashMap;

#[derive(Debug, PartialEq)]
pub struct MySQLTypeMap {
    kafka_connect_map: HashMap<&'static str, &'static str>,
}

impl Default for MySQLTypeMap {
    fn default() -> Self {
        Self::new()
    }
}

impl MySQLTypeMap {
    pub fn new() -> Self {
        let kafka_connect_map = Self::get_kafka_connect_map();
        MySQLTypeMap { kafka_connect_map }
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
