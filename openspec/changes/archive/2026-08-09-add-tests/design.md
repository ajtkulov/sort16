## Context

sort16 baseline specs (`record-format`, `external-sort`, `k-way-merge`, `cli`) describe current behavior, but `src/test` is empty. Production code is file-centric (`Batch`, `MergeSort`, `Main.sortFile`) with comparison logic embedded in `Batch.internalSort` and `RecordWrap.ordering`. Existing deps already include ScalaTest and ZIO Test; `parallelExecution in Test := false` and `fork := true` are set.

## Goals / Non-Goals

**Goals:**
- Maximize automated coverage of baseline scenarios with both in-memory and file-based tests
- Keep product behavior unchanged except for minimal testability extracts if needed
- Make `sbt test` a practical regression gate on temp-file fixtures with tiny block/buffer sizes

**Non-Goals:**
- Process-level CLI smoke via `sbt run` / assembly
- Performance or multi-GB fixtures
- Concurrency/stress proving the semaphore limit
- Rewriting the sorter to a pure in-memory architecture
- Fixing README drift

## Decisions

### 1. ScalaTest as primary style; ZIO Test optional
- **Choice:** Write suites with ScalaTest (`AnyFlatSpec` / `AnyFunSuite` + matchers), matching the simplest path for `Conf`, compare, and temp-file I/O.
- **Why:** Most coverage is synchronous JVM I/O and pure compare; ZIO only wraps `sortFile` batch execution.
- **Alternatives:** ZIO Test everywhere — more ceremony for little gain; keep ScalaTest for sync paths and only use ZIO Test if asserting on ZIO effects directly.

### 2. Shared `RecordIo` / compare helpers in test (and thin production extract)
- **Choice:** Add test helpers to pack `(i0,i1,i2,i3)` → 16 bytes, write/read `.dat` files, assert sorted + same multiset. Prefer extracting a shared `compareRecords` (or equivalent) used by both `Batch` and `RecordWrap` so in-memory tests hit production ordering code.
- **Why:** Duplicating compare in tests drifts from production; today’s two copies already risk divergence.
- **Alternatives:** Test only via `RecordWrap.ordering` without extract — acceptable fallback if extract proves noisy; still cover Batch path via file tests.

### 3. File tests use temp dirs + tiny sizes
- **Choice:** `Files.createTempDirectory`, `blocksize` like 32–64 bytes (2–4 records), `readbuffersize` like 16–32 bytes to force merge refill, `threads` = 1 or 2 for determinism.
- **Why:** Exercises real external-sort and merge refill paths without large fixtures.
- **Alternatives:** Mock `RandomAccessFile` — higher coupling, less confidence.

### 4. Call library entry points, not `App.main`
- **Choice:** Invoke `Main.sortFile`, `MergeSort`, `Batch`/`Conf` directly from tests; optionally mirror merge-only by constructing `MergeSort` (same as non-`sort` action).
- **Why:** Avoids process spawning and Scallop/`App` lifecycle quirks while still covering the real pipelines.
- **Alternatives:** Full process CLI tests — deferred (non-goal).

### 5. Suite layout
```
src/test/scala/sort16/
  support/RecordIo.scala
  RecordFormatSpec.scala      # in-memory order
  ConfSpec.scala              # CLI defaults / missing output
  ExternalSortFileSpec.scala  # partition, multi-file, cleanup, source intact
  MergeSortFileSpec.scala     # merge, refill, merge-only
  EndToEndSortFileSpec.scala  # sortFile → merge → assert (can fold into External if leaner)
  InvalidRecordLengthSpec.scala
```
Folding EndToEnd into ExternalSort is fine if it reduces boilerplate; coverage matters more than file count.

### 6. Invalid length assertion
- **Choice:** Write a file with length ≢ 0 (mod 16) and assert `AssertionError` (or failure) from `Batch.read` / `FileIterator` / pipeline.
- **Why:** Matches baseline “fail rather than process partial record.”

## Risks / Trade-offs

- **[Risk] Extracting compare changes production code lightly** → Mitigation: keep behavior identical; file tests still guard Batch path; keep extract minimal (one function).
- **[Risk] Temp-file races / leftover files** → Mitigation: unique temp dirs, `afterAll`/`try/finally` cleanup; `parallelExecution := false` already set.
- **[Risk] `Conf.verify()` / Scallop exit behavior hard to catch** → Mitigation: use Scallop’s exception-throwing mode if available, or construct `Conf` carefully; if missing-output is awkward, document and cover defaults thoroughly first.
- **[Risk] Run files named `{inputPath}.{idx}` litter the input directory** → Mitigation: put inputs inside the temp dir so runs land there and cleanup assertions are local.
- **[Trade-off] No process-level CLI coverage** → Acceptable for v1; library-level paths cover sort/merge behavior.

## Migration Plan

1. Land helpers + in-memory specs
2. Land file-based suites
3. Confirm `sbt test` green
4. No runtime migration; rollback = delete test sources / revert extract

## Open Questions

- None blocking: if Scallop missing-`--output` is hard to assert cleanly, ship defaults tests and treat missing-output as best-effort.
