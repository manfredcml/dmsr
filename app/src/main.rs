mod args;

use std::thread;
use args::Args;
use clap::Parser;
use log::{error, info};
use dms::sources::config::Config;
use dms::sources::postgres::PostgresSource;
use dms::sources::source::Source;
use anyhow::Result;
use dms::events::event_stream::DataStream;
use futures::executor::block_on;

#[tokio::main]
async fn main() -> Result<()> {
  env_logger::init();

  info!("Starting up...");

  let args = Args::parse();
  let source = args.source.as_str();

  match source {
    "postgres" => {
      let mut source = PostgresSource::new(Config {
        endpoint: "host=localhost user=postgres password=postgres dbname=postgres replication=database".to_string(),
      });
      source.connect().await?;

      let mut event_stream = DataStream::new();

      let send_thread = thread::spawn(move || {
        block_on(source.stream(&mut event_stream.tx))
      });

      let receive_thread = thread::spawn(move || {
        loop {
          let event = block_on(event_stream.rx.recv());
          println!("Here you go - {:?}", event);
        }
      });

      let _ = send_thread.join();
      let _ = receive_thread.join();

      // Block on the event stream
      // while let Some(event) = event_stream.rx.next().await {
      //   println!("{:?}", event);
      // }
    }
    _ => {
      error!("Unknown source: {}", source);
    }
  }

  info!("Terminating...");
  Ok(())
}
