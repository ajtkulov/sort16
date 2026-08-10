# Sort16

External sorter for fixed 16-byte binary records. Partitions inputs into blocks, sorts batches in parallel, then k-way merges sorted runs into one output file.

Two implementations live in this repo:

- **Scala** — `src/main/scala/sort16/` (ZIO batch parallelism)
- **Rust** — `rust/` (Rayon batch parallelism; behavioral parity)

## Build Scala assembly

Requires sbt with the [sbt-assembly](https://github.com/sbt/sbt-assembly) plugin (`project/plugins.sbt`).

```bash
sbt assembly
```

Produces:

```text
target/scala-2.13/sort16.jar
```

## Build Rust release

```bash
cd rust && cargo build --release
```

Binary:

```text
rust/target/release/sort16
```

## Run (Scala jar)

Trailing args are input paths; `--output` is required.

```bash
java -Xmx16G -jar target/scala-2.13/sort16.jar \
  --output sorted.dat \
  file1.dat file2.dat
```

## Run (Rust)

```bash
./rust/target/release/sort16 \
  --output sorted.dat \
  file1.dat file2.dat
```

Shell globs expand before the process (not inside the program):

```bash
./rust/target/release/sort16 --output sorted.dat *.dat
```

With tuning flags:

```bash
./rust/target/release/sort16 \
  --output sorted.dat \
  --blocksize 1000000000 \
  --threads 12 \
  --readbuffersize 20000000 \
  file1.dat
```

Merge already-sorted runs only:

```bash
./rust/target/release/sort16 \
  --output merged.dat \
  --action merge \
  run0.dat run1.dat
```

## Run via sbt (Scala dev)

```bash
sbt "run --output sorted.dat file1.dat file2.dat"
```

`build.sbt` sets `-Xmx16G` for `run`.

## Parameters

| Flag | Default | Description |
|------|---------|-------------|
| trailing args | (required) | Input file paths |
| `--output` | (required) | Merged sorted output path |
| `--blocksize` | `1000000000` | Batch size in bytes |
| `--threads` | `12` | Max concurrent batch pipelines |
| `--readbuffersize` | `20000000` | Per-run merge read buffer (bytes) |
| `--action` | `sort` | `sort` = batch sort + merge; any other value = merge-only |

## Tests

```bash
# Scala
sbt test

# Rust
cd rust && cargo test
```

## 1GiB stress comparison (Scala vs Rust)

See [docs/stress-1g.md](docs/stress-1g.md). Short version:

```bash
./scripts/stress_1g.sh
```

Generates a 1 GiB fixture (if missing), times both implementations with the same parameters, verifies sorted order, and writes `target/stress/results-*.md`.

## How it works

1. **Partition** — each input is split into `blocksize` byte ranges (batches)
2. **Parallel batch sort** — each batch is index-sorted and written as a run file `{input}.{batchIndex}`
3. **K-way merge** — a priority queue (one head per run) merges runs into `--output`, using large sequential read buffers
4. **Cleanup** — temporary run files are deleted after a successful full sort
