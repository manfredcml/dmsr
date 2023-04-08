use crate::error::generic::{DMSRError, DMSRResult};
use chrono::NaiveDateTime;
use strum_macros::{EnumString, EnumVariantNames};

#[derive(Debug)]
pub enum PgOutputEvent {
    Relation(RelationEvent),
    Insert(InsertEvent),
    Begin(BeginEvent),
    Commit(CommitEvent),
    Update(UpdateEvent),
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
    pub tuple_type: TupleType,
    pub num_columns: u16,
    pub columns: Vec<ColumnData>,
}

#[derive(Debug)]
pub struct UpdateEvent {
    pub timestamp: NaiveDateTime,
    pub relation_id: u32,
    pub tuple_type: TupleType,
    pub num_columns: u16,
    pub columns: Vec<ColumnData>,
}

#[derive(Debug)]
pub struct ColumnData {
    pub column_data_category: ColumnDataCategory,
    pub column_data_length: Option<u32>,
    pub column_value: Option<String>,
}

#[derive(Debug, PartialEq, EnumString, EnumVariantNames)]
pub enum TupleType {
    #[strum(serialize = "O")]
    Old,
    #[strum(serialize = "K")]
    Key,
    #[strum(serialize = "N")]
    New,
}

#[derive(Debug, PartialEq, EnumString, EnumVariantNames)]
pub enum ColumnDataCategory {
    #[strum(serialize = "n")]
    Null,
    #[strum(serialize = "u")]
    Unknown,
    #[strum(serialize = "t")]
    Text,
}

#[derive(Debug, PartialEq, EnumString, EnumVariantNames)]
pub enum MessageType {
    #[strum(serialize = "R")]
    Relation,
    #[strum(serialize = "I")]
    Insert,
    #[strum(serialize = "B")]
    Begin,
    #[strum(serialize = "C")]
    Commit,
    #[strum(serialize = "U")]
    Update,
}

#[derive(Debug, PartialEq, EnumString, EnumVariantNames)]
pub enum ReplicationIdentity {
    #[strum(serialize = "d")]
    Default,
}

#[derive(Debug)]
pub struct RelationColumn {
    pub is_pk: bool,
    pub column_name: String,
    pub column_type: u32,
    pub column_type_modifier: i32,
}
