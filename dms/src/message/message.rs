use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct KafkaMessage<Source> {
    pub schema: Schema,
    pub payload: Payload<Source>,
}

impl<Source> KafkaMessage<Source> {
    pub fn new(schema: Schema, payload: Payload<Source>) -> Self {
        KafkaMessage { schema, payload }
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct Schema {
    pub r#type: String,
    pub fields: Vec<Field>,
    pub optional: bool,
    pub name: String,
}

impl Schema {
    pub fn new<N>(before: Field, after: Field, source: Field, name: N) -> Self
    where
        N: Into<String>,
    {
        let mut fields = vec![before, after, source];
        fields.append(&mut Self::get_standard_field_schema());

        Schema {
            r#type: "struct".into(),
            fields,
            optional: false,
            name: name.into(),
        }
    }

    fn get_standard_field_schema() -> Vec<Field> {
        vec![
            Field::new("string", false, "op", None),
            Field::new("int64", false, "ts_ms", None),
        ]
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct Field {
    pub r#type: String,
    pub optional: bool,
    pub field: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<Field>>,
}

impl Field {
    pub fn new<T, F>(r#type: T, optional: bool, field: F, fields: Option<Vec<Field>>) -> Self
    where
        T: Into<String>,
        F: Into<String>,
    {
        Field {
            r#type: r#type.into(),
            optional,
            field: field.into(),
            fields,
        }
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct Payload<Source> {
    pub before: Option<serde_json::Value>,
    pub after: Option<serde_json::Value>,
    pub op: Operation,
    pub ts_ms: i64,
    pub source: Source,
}

impl<Source> Payload<Source> {
    pub fn new(
        before: Option<serde_json::Value>,
        after: Option<serde_json::Value>,
        op: Operation,
        ts_ms: i64,
        source: Source,
    ) -> Self {
        Payload {
            before,
            after,
            op,
            ts_ms,
            source,
        }
    }
}

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
}
