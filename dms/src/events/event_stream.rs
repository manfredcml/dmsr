use futures::channel::mpsc::{Receiver, Sender};
use crate::streamers::event::Event;

pub struct DataStream {
  tx: Sender<Event>,
  rx: Receiver<Event>,
}