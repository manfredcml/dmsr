use crate::error::generic::{DMSRError, DMSRResult};
use chrono::NaiveDateTime;

#[derive(Debug)]
pub enum PgOutputEvent {
    Relation(RelationEvent),
    Insert(InsertEvent),
}

#[derive(Debug)]
pub struct RelationEvent {
    pub timestamp: NaiveDateTime,
    pub relation_id: u32,
    pub namespace: String,
    pub relation_name: String,
    pub repl_identity: ReplicationIdentity,
    pub num_columns: u16,
    pub columns: Vec<RelationColumn>,
}

#[derive(Debug)]
pub struct InsertEvent {
    pub timestamp: NaiveDateTime,
    pub relation_id: u32,
    pub tuple_type: char,
    pub num_columns: u16,
    pub values: Vec<String>,
}

#[derive(Debug, PartialEq)]
pub enum MessageType {
    Relation,
    Insert,
}

impl MessageType {
    pub fn from_char(c: char) -> DMSRResult<Self> {
        match c {
            'R' => Ok(MessageType::Relation),
            'I' => Ok(MessageType::Insert),
            _ => Err(DMSRError::PostgresError("Unknown message type".into())),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum ReplicationIdentity {
    Default,
}

impl ReplicationIdentity {
    pub fn from_char(c: char) -> DMSRResult<Self> {
        match c {
            'd' => Ok(ReplicationIdentity::Default),
            _ => Err(DMSRError::PostgresError(
                "Unknown replication identity".into(),
            )),
        }
    }
}

#[derive(Debug)]
pub struct RelationColumn {
    pub is_pk: bool,
    pub column_name: String,
    pub column_type: u32,
    pub column_type_modifier: i32,
}
