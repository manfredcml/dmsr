use crate::error::generic::{DMSRError, DMSRResult};
use chrono::NaiveDateTime;

#[derive(Debug)]
pub enum PgOutputEvent {
    Relation(RelationEvent),
    Insert(InsertEvent),
    Begin(BeginEvent),
    Commit(CommitEvent),
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
pub struct BeginEvent {
    pub timestamp: NaiveDateTime,
    pub lsn: u64,
    pub commit_timestamp: NaiveDateTime,
    pub tx_xid: u32,
}

#[derive(Debug)]
pub struct CommitEvent {
    pub timestamp: NaiveDateTime,
    pub flags: u8,
    pub lsn_commit: u64,
    pub lsn: u64,
    pub commit_timestamp: NaiveDateTime,
}

#[derive(Debug)]
pub struct InsertEvent {
    pub timestamp: NaiveDateTime,
    pub relation_id: u32,
    pub tuple_type: char,
    pub num_columns: u16,
    pub values: Vec<String>,
}

#[derive(Debug)]
pub struct UpdateEvent {}

#[derive(Debug, PartialEq)]
pub enum MessageType {
    Relation,
    Insert,
    Begin,
    Commit
}

impl MessageType {
    pub fn from_char(c: char) -> DMSRResult<Self> {
        match c {
            'R' => Ok(MessageType::Relation),
            'I' => Ok(MessageType::Insert),
            'B' => Ok(MessageType::Begin),
            'C' => Ok(MessageType::Commit),
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
