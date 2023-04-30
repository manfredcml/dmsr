use crate::error::error::{DMSRError, DMSRResult};
use std::collections::HashMap;

#[derive(Debug, PartialEq)]
pub struct PostgresKafkaConnectTypeMap {
    pub map: HashMap<&'static str, &'static str>,
}

impl Default for PostgresKafkaConnectTypeMap {
    fn default() -> Self {
        Self::new()
    }
}

impl PostgresKafkaConnectTypeMap {
    pub fn new() -> Self {
        let mut map: HashMap<&'static str, &'static str> = HashMap::new();
        map.insert("bool", "boolean");
        map.insert("boolean", "boolean");
        map.insert("char", "string");
        map.insert("character", "string");
        map.insert("bpchar", "string");
        map.insert("enum", "string");
        map.insert("json", "string");
        map.insert("jsonb", "string");
        map.insert("inet", "string");
        map.insert("text", "string");
        map.insert("uuid", "string");
        map.insert("varchar", "string");
        map.insert("character varying", "string");
        map.insert("interval", "string");
        map.insert("smallint", "int16");
        map.insert("integer", "int32");
        map.insert("int", "int32");
        map.insert("int2", "int16");
        map.insert("int4", "int32");
        map.insert("date", "date");
        map.insert("time", "time");
        map.insert("bigint", "int64");
        map.insert("int8", "int64");
        map.insert("timestamp", "timestamp");
        map.insert("timestamptz", "timestamp");
        map.insert("timestamp without time zone", "timestamp");
        map.insert("timestamp with time zone", "timestamp");
        map.insert("real", "float32");
        map.insert("float4", "float32");
        map.insert("float8", "float64");
        map.insert("double precision", "float64");
        map.insert("double_precision", "float64");
        map.insert("array", "array");
        map.insert("daterange", "string");
        map.insert("int4range", "string");
        map.insert("int2vector", "array");
        map.insert("numeric", "decimal");
        map.insert("decimal", "decimal");
        PostgresKafkaConnectTypeMap { map }
    }

    pub fn get(&self, key: &str) -> DMSRResult<String> {
        let avro_type =
            self.map
                .get(key)
                .map(|v| v.to_string())
                .ok_or(DMSRError::PostgresError(format!(
                    "No Kafka connect type found for Postgres type: {}",
                    key
                )))?;
        Ok(avro_type)
    }
}
