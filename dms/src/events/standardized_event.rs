use crate::events::event_type::EventType;
use crate::sources::source_type::SourceType;

#[derive(Debug)]
pub struct Event {
  pub source_type: SourceType,
  pub event_type: EventType,
  pub schema: String,
  pub table: String,
}