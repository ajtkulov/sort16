## 1. Test helpers and compare extract

- [x] 1.1 Extract shared record compare used by `Batch.internalSort` and `RecordWrap.ordering` (behavior-preserving)
- [x] 1.2 Add `src/test/scala/sort16/support/RecordIo.scala` helpers: pack 4 ints → 16 bytes, write/read `.dat`, assert sorted + same multiset, temp-dir fixture cleanup

## 2. In-memory suites

- [x] 2.1 Add `RecordFormatSpec`: differ at int 0/1/2/3, equal records, signed high-bit keys
- [x] 2.2 Add `ConfSpec`: defaults for blocksize/threads/readbuffersize/action; missing `--output` rejection (best-effort if Scallop is awkward)

## 3. File-based external sort suites

- [x] 3.1 Add external/end-to-end file tests: single-block unsorted → ordered output via `sortFile` + `MergeSort`
- [x] 3.2 Cover multi-block with tiny `blocksize`, multi-file inputs, source bytes unchanged, temp run cleanup after merge

## 4. File-based merge and failure suites

- [x] 4.1 Add `MergeSortFileSpec`: merge multiple sorted runs; tiny `readbuffersize` refill; merge-only path without batch partition
- [x] 4.2 Add invalid-length test: file size not multiple of 16 fails on read/pipeline assert

## 5. Verification

- [x] 5.1 Run `sbt test` and fix failures until green
- [x] 5.2 Confirm coverage maps to `testing` delta scenarios (in-memory order/CLI, external-sort, merge/refill, invalid length)
