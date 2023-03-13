use crate::event::kind::EventKind;
use serde::{Deserialize, Serialize};
use crate::connector::kind::ConnectorKind;

#[derive(Debug, Deserialize, Serialize)]
pub struct ChangeEvent {
    pub source_name: String,
    pub source_kind: ConnectorKind,
    pub event_kind: EventKind,
}
