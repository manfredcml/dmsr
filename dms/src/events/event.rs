use crate::events::kind::EventKind;
use crate::sources_targets::source_kind::SourceKind;
use serde::{Deserialize, Serialize};
use crate::events::postgres_event::PostgresEvent;

#[derive(Debug, Deserialize, Serialize)]
pub struct ChangeEvent {
    pub source_name: String,
    pub source_kind: SourceKind,
    pub event_kind: EventKind,

    pub postgres_event: Option<PostgresEvent>,
}
