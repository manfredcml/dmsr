use crate::events::event_type::EventType;
use crate::sources::source_kind::SourceKind;

#[derive(Debug)]
pub struct Event {
  pub source_type: SourceKind,
  pub event_type: EventType,
  pub schema: String,
  pub table: String,
}