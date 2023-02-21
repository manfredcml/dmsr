mod args;

use args::Args;
use clap::Parser;
use log::{info};

fn main() {
  env_logger::init();

  info!("run begins");
  let args = Args::parse();

  for _ in 0..args.count {
    println!("Hello, {}!", args.name);
  }
}
