# sort16 (Rust)

Rust port of the sort16 external sorter for fixed **16-byte** binary records. Same CLI shape and defaults as the Scala implementation: partition by block → parallel batch index-sort → k-way merge (one heap head per run).

## Requirements

- Rust toolchain (Cargo / `rustc`); edition 2021

## Compile

From this directory:

```bash
# Debug
cargo build

# Release (recommended for real data)
cargo build --release
```

Binaries:

| Profile | Path |
|---------|------|
| debug | `target/debug/sort16` |
| release | `target/release/sort16` |

From the repo root:

```bash
cargo build --release --manifest-path rust/Cargo.toml
# → rust/target/release/sort16
```

## Run

`--output` is required. Input paths are trailing arguments.

```bash
./target/release/sort16 \
  --output sorted.dat \
  file1.dat
```

Multiple inputs:

```bash
./target/release/sort16 \
  --output sorted.dat \
  file1.dat file2.dat file3.dat
```

Shell globs expand in the shell (not inside the program):

```bash
./target/release/sort16 --output sorted.dat *.dat
```

Dev run without installing the binary:

```bash
cargo run --release -- \
  --output sorted.dat \
  file1.dat
```

## Parameters

| Flag | Default | Description |
|------|---------|-------------|
| trailing args | (required) | Input file paths |
| `--output` | (required) | Merged sorted output path |
| `--blocksize` | `1000000000` | Batch size in bytes |
| `--threads` | `12` | Max concurrent batch pipelines (Rayon pool size) |
| `--readbuffersize` | `20000000` | Per-run merge read buffer (bytes) |
| `--action` | `sort` | `sort` = batch sort + merge; any other value = merge-only of already-sorted runs |

### Examples with tuning

```bash
./target/release/sort16 \
  --output sorted.dat \
  --blocksize 268435456 \
  --threads 4 \
  --readbuffersize 20971520 \
  input.dat
```

Merge-only (inputs must already be sorted runs):

```bash
./target/release/sort16 \
  --output merged.dat \
  --action merge \
  run0.dat run1.dat
```

Help:

```bash
./target/release/sort16 --help
```

## Tests

```bash
cargo test
```

## 1 GiB fixture (optional)

```bash
cargo run --release --example gen_1g -- /path/to/input-1g.dat
```

Writes exactly 1 GiB (`2^30` bytes) of 16-byte records. Repo-level stress comparison vs Scala: `../scripts/stress_1g.sh` (see `../docs/stress-1g.md`).
