use clap::Parser;
use sort16::cli::{run_from_args, Args};

fn main() {
    let args = Args::parse();
    if let Err(e) = run_from_args(args) {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}
