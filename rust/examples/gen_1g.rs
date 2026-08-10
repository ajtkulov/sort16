//! Generate a ~1 GiB (2^30 bytes) file of random-ish 16-byte records.
//!
//! Usage: cargo run --release --example gen_1g -- /path/to/out.dat

use std::env;
use std::fs::File;
use std::io::{BufWriter, Write};

const RECORD_SIZE: usize = 16;
const TARGET_BYTES: u64 = 1 << 30; // 1 GiB
const RECORDS: u64 = TARGET_BYTES / RECORD_SIZE as u64;

fn main() {
    let path = env::args()
        .nth(1)
        .unwrap_or_else(|| "target/stress/input-1g.dat".to_string());
    if let Some(parent) = std::path::Path::new(&path).parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let file = File::create(&path).expect("create");
    let mut out = BufWriter::with_capacity(16 * 1024 * 1024, file);
    let mut state: u64 = 0x1234_5678_9abc_def0;
    let mut buf = [0u8; RECORD_SIZE];
    for i in 0..RECORDS {
        // xorshift64* style mix so keys are unsorted
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let v = state.wrapping_mul(0x2545F4914F6CDD1D).wrapping_add(i);
        buf[0..8].copy_from_slice(&v.to_be_bytes());
        buf[8..16].copy_from_slice(&(v.wrapping_mul(0x9E3779B97F4A7C15)).to_be_bytes());
        out.write_all(&buf).expect("write");
        if i % 8_000_000 == 0 && i > 0 {
            eprintln!("wrote {} / {} records", i, RECORDS);
        }
    }
    out.flush().expect("flush");
    eprintln!("wrote {} bytes to {}", TARGET_BYTES, path);
}
