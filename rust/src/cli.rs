use std::path::PathBuf;

use clap::Parser;

use crate::sort;

pub const DEFAULT_BLOCK_SIZE: usize = 1_000_000_000;
pub const DEFAULT_THREADS: usize = 12;
pub const DEFAULT_READ_BUFFER_SIZE: usize = 20_000_000;

#[derive(Parser, Debug)]
#[command(name = "sort16", about = "External sorter for 16-byte binary records")]
pub struct Args {
    /// Output file for the merged sorted result
    #[arg(long)]
    pub output: PathBuf,

    /// Batch block size in bytes
    #[arg(long, default_value_t = DEFAULT_BLOCK_SIZE)]
    pub blocksize: usize,

    /// Max concurrent batch pipelines
    #[arg(long, default_value_t = DEFAULT_THREADS)]
    pub threads: usize,

    /// Per-run merge read buffer size in bytes
    #[arg(long, default_value_t = DEFAULT_READ_BUFFER_SIZE)]
    pub readbuffersize: usize,

    /// `sort` = batch sort + merge; any other value = merge-only
    #[arg(long, default_value = "sort")]
    pub action: String,

    /// Input file paths
    #[arg(required = true)]
    pub files: Vec<PathBuf>,
}

pub fn run_from_args(args: Args) -> std::io::Result<()> {
    println!(
        "params, files={}, blockSize={}, threads={}, output={}",
        args.files
            .iter()
            .map(|p| p.display().to_string())
            .collect::<Vec<_>>()
            .join(","),
        args.blocksize,
        args.threads,
        args.output.display()
    );
    sort::run_sort(
        &args.files,
        &args.output,
        args.blocksize,
        args.threads,
        args.readbuffersize,
        &args.action,
    )
}
