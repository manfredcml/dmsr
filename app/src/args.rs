pub use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
  /// Name of the person to greet
  #[arg(short, long, default_value = "postgres")]
  pub source: String,
}
