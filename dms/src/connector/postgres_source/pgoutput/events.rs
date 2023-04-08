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
    Delete(DeleteEvent),
    Truncate(TruncateEvent),
    Origin,
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
pub struct DeleteEvent {
    pub timestamp: NaiveDateTime,
    pub relation_id: u32,
    pub tuple_type: TupleType,
    pub num_columns: u16,
    pub columns: Vec<ColumnData>,
}

#[derive(Debug)]
pub struct TruncateEvent {
    pub timestamp: NaiveDateTime,
    pub num_relations: u32,
    pub option_bits: TruncateOptionBit,
    pub relation_ids: Vec<u32>,
}

#[derive(Debug)]
pub struct ColumnData {
    pub column_data_category: ColumnDataCategory,
    pub column_data_length: Option<u32>,
    pub column_value: Option<String>,
}

#[derive(Debug, PartialEq)]
pub enum TruncateOptionBit {
    None,
    Cascade,
    RestartIdentity,
}

impl TruncateOptionBit {
    pub fn from_u8(value: u8) -> DMSRResult<Self> {
        match value {
            0 => Ok(TruncateOptionBit::None),
            1 => Ok(TruncateOptionBit::Cascade),
            2 => Ok(TruncateOptionBit::RestartIdentity),
            _ => Err(DMSRError::PostgresError(
                "Unknown truncate option bit".into(),
            )),
        }
    }
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
    #[strum(serialize = "D")]
    Delete,
    #[strum(serialize = "T")]
    Truncate,
    #[strum(serialize = "O")]
    Origin,
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
