## Purpose

Define how sort16 partitions inputs into memory-bounded batches, sorts each batch, and writes sorted run files for later merging.

## Requirements

### Requirement: Block Partition Across Inputs
When action is full sort, the system SHALL partition every input file into contiguous byte ranges of configured `blocksize`, covering the whole file from offset 0.

#### Scenario: Multiple full blocks
- **WHEN** an input file size is greater than `blocksize` and not an exact multiple
- **THEN** the system SHALL create one batch per full block plus one final batch for the remaining bytes

#### Scenario: Single-block file
- **WHEN** an input file size is less than or equal to `blocksize`
- **THEN** the system SHALL create exactly one batch for that file

#### Scenario: Multiple input files
- **WHEN** more than one input file is provided
- **THEN** batches from all input files SHALL be collected into one combined batch list before parallel sorting

### Requirement: In-Memory Index Sort Per Batch
For each batch, the system SHALL read the batch bytes, sort record indices by the record-format total order, and write records in sorted order without mutating the source file.

#### Scenario: Batch pipeline
- **WHEN** a batch is processed
- **THEN** the system SHALL read the range, sort indices by record comparison, write the sorted records to a run file, release batch buffers, and close the batch's file handle

#### Scenario: Source files unchanged
- **WHEN** batch sorting completes
- **THEN** each original input file SHALL remain unmodified

### Requirement: Sorted Run File Naming
Each sorted batch SHALL be written to a run file named `{inputPath}.{batchIndex}` where `batchIndex` is the zero-based index of the batch in the combined batch list across all inputs.

#### Scenario: Run path derived from input
- **WHEN** a batch originates from input path `/data/a.dat` and is the third batch overall (index 2)
- **THEN** its run file SHALL be `/data/a.dat.2`

### Requirement: Bounded Parallel Batch Processing
The system SHALL process batches concurrently with a configurable concurrency limit so that at most `threads` batch pipelines run at once.

#### Scenario: Default concurrency
- **WHEN** `--threads` is not provided
- **THEN** the concurrency limit SHALL default to 12

#### Scenario: Custom concurrency
- **WHEN** `--threads N` is provided
- **THEN** at most `N` batches SHALL execute their pipelines concurrently

### Requirement: Temporary Runs Returned For Merge
After all batches complete, the full-sort path SHALL produce the ordered list of run file paths for the k-way merge stage and SHALL delete those run files after a successful merge to the final output.

#### Scenario: Cleanup after merge
- **WHEN** full sort merges all run files into the final output successfully
- **THEN** each temporary run file SHALL be deleted
