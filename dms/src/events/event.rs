use crate::events::event_kind::EventKind;
use crate::sources::source_kind::SourceKind;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct ChangeEvent {
    pub source_name: String,
    pub source_kind: SourceKind,
    pub event_kind: EventKind,
    pub schema: String,
    pub table: String,
}
