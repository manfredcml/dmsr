use crate::sources::source_type::SourceType;
use crate::streamers::event_type::EventType;

pub struct Event {
  source_type: SourceType,
  event_type: EventType,
  schema: String,
  table: String,
}