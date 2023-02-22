use tokio::sync::mpsc::{Receiver, Sender, channel};
use crate::events::standardized_event::Event;

pub struct DataStream {
  pub tx: Sender<Event>,
  pub rx: Receiver<Event>,
}

impl DataStream {
  pub fn new() -> Self {
    let (tx, rx) = channel(
      100
    );
    DataStream { tx, rx }
  }
}