## Purpose

Automated in-memory and file-based verification that the baseline sort16 capabilities (record-format, external-sort, k-way-merge, cli) behave as specified.

## Requirements

### Requirement: In-Memory Record Order Tests
The project SHALL provide automated in-memory tests that verify the record-format total order without requiring disk I/O for the comparison itself.

#### Scenario: Differ at each integer position
- **WHEN** pairs of 16-byte records differ only at integer index 0, 1, 2, or 3
- **THEN** the tests SHALL assert that the smaller signed integer at that index orders first

#### Scenario: Equal records
- **WHEN** two records have identical four integers
- **THEN** the tests SHALL assert that comparison treats them as equal

#### Scenario: Signed high-bit keys
- **WHEN** a record contains `0xFFFFFFFF` (or another high-bit pattern) in a compared integer slot
- **THEN** the tests SHALL assert signed ordering (negative before non-negative when that slot decides)

### Requirement: In-Memory CLI Conf Tests
The project SHALL provide automated tests for CLI configuration defaults and required arguments without running a full sort.

#### Scenario: Default tuning values
- **WHEN** `Conf` is constructed with trailing inputs and `--output` only
- **THEN** the tests SHALL assert defaults `blocksize=1000000000`, `threads=12`, `readbuffersize=20000000`, and `action=sort` (via `getOrElse` / parsed values as used by `Main`)

#### Scenario: Missing output rejected
- **WHEN** `Conf` is constructed without `--output`
- **THEN** the tests SHALL assert that verification fails (argument parsing does not succeed)

### Requirement: File-Based External Sort Tests
The project SHALL provide automated file-based tests that exercise batch partitioning, sorting, and temporary run behavior using temporary directories and small `blocksize` values.

#### Scenario: Single-block sort produces ordered output
- **WHEN** an unsorted input file fits in one block and is sorted end-to-end
- **THEN** the output SHALL contain the same records in non-decreasing record order

#### Scenario: Multi-block partition and merge
- **WHEN** an input is larger than a tiny `blocksize` spanning multiple batches
- **THEN** the end-to-end sort SHALL still produce globally ordered output with all records preserved

#### Scenario: Multi-file inputs
- **WHEN** two or more input files are sorted together
- **THEN** the output SHALL contain the union of all records in non-decreasing order

#### Scenario: Source files unchanged
- **WHEN** external sort completes
- **THEN** each original input file's bytes SHALL match their pre-sort contents

#### Scenario: Temporary runs cleaned up
- **WHEN** full sort completes successfully
- **THEN** temporary `{inputPath}.{batchIndex}` run files SHALL no longer exist

### Requirement: File-Based Merge Tests
The project SHALL provide automated file-based tests for k-way merge, including merge-only mode and chunk refill.

#### Scenario: Merge sorted runs
- **WHEN** two or more already-sorted run files are merged
- **THEN** the output SHALL be globally ordered and contain exactly the total input record count

#### Scenario: Tiny read buffer forces refill
- **WHEN** merge runs with a `readbuffersize` small enough that a run requires multiple refills
- **THEN** the merged output SHALL still be complete and correctly ordered

#### Scenario: Merge-only skips batch sort
- **WHEN** merge is invoked on pre-sorted runs without the batch-sort stage (non-`sort` action path or direct `MergeSort` usage)
- **THEN** the tests SHALL verify ordered merge output without creating unsorted-partition batch runs from those inputs

### Requirement: Invalid Record Length Failure Test
The project SHALL include a file-based test that exercises failure when a read length is not divisible by 16.

#### Scenario: Truncated file rejected
- **WHEN** an input file whose length is not a multiple of 16 is processed by a code path that asserts alignment
- **THEN** the test SHALL assert that processing fails (assertion or equivalent hard failure)
