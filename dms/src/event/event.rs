use crate::event::kind::EventKind;
use crate::source_connector::kind::SourceConnectorKind;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct ChangeEvent {
    pub source_name: String,
    pub source_kind: SourceConnectorKind,
    pub event_kind: EventKind,
}
