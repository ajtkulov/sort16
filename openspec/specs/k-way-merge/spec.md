## Purpose

Define how sort16 merges already-sorted run files into a single totally ordered output stream using a priority queue.

## Requirements

### Requirement: Multi-Way Merge Of Sorted Runs
The system SHALL merge one or more sorted run files into a single output file such that the output is totally ordered by the record-format comparison.

#### Scenario: Merge produces global order
- **WHEN** each input run is individually sorted in ascending record order
- **THEN** the merged output SHALL contain all records from all runs in non-decreasing order

#### Scenario: All records preserved
- **WHEN** runs containing a total of `N` records are merged
- **THEN** the output file SHALL contain exactly `N` 16-byte records

### Requirement: Priority-Queue Merge With Chunked Reads
The merge SHALL keep a priority queue of at most one head record per run, backed by per-run read buffers of configurable `readbuffersize`. Large sequential reads fill each run's buffer; the heap only holds the current head. When a run's head is emitted, the merge advances within that buffer or refills from disk when the buffer is exhausted.

#### Scenario: Initial load
- **WHEN** merge initializes
- **THEN** for each non-empty run file the system SHALL read up to `readbuffersize` bytes into a buffer and enqueue only that run's first record

#### Scenario: Advance within buffer
- **WHEN** a run's head is written and more records remain in that run's current buffer
- **THEN** the system SHALL enqueue the next record from the same buffer without reading from disk

#### Scenario: Refill after buffer exhausted
- **WHEN** the last record of a run's current buffer is written to the output and more bytes remain in that run
- **THEN** the system SHALL read the next buffer from the next file offset and enqueue that buffer's first record

#### Scenario: Run exhausted
- **WHEN** the last record of a run's current buffer is written and the run has no remaining bytes
- **THEN** the system SHALL stop reading that run and continue merging from remaining runs

### Requirement: Default Read Buffer Size
When `--readbuffersize` is omitted, the merge read buffer SHALL default to 20,000,000 bytes.

#### Scenario: Default buffer
- **WHEN** merge runs without `--readbuffersize`
- **THEN** each run refill SHALL use a 20,000,000-byte read buffer

### Requirement: Merge-Only Mode
When CLI action is not `sort`, the system SHALL treat the provided input paths as already-sorted runs and merge them directly into `--output` without the external-sort batch stage.

#### Scenario: Pre-sorted inputs
- **WHEN** `--action` is set to a value other than `sort` and input files are sorted runs
- **THEN** the system SHALL merge those files into the output path and SHALL NOT create batch run files from unsorted partitioning
