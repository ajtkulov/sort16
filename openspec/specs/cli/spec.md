## Purpose

Define the sort16 command-line interface: required arguments, optional tuning flags, defaults, and the full-sort versus merge-only control flow.

## Requirements

### Requirement: Required Output And Input Trails
The CLI SHALL accept one or more input file paths as trailing arguments and SHALL require an `--output` path for the merged result.

#### Scenario: Full sort invocation
- **WHEN** the user runs the program with trailing input paths and `--output out.dat`
- **THEN** the system SHALL sort and merge those inputs into `out.dat`

#### Scenario: Missing output
- **WHEN** `--output` is not provided
- **THEN** argument parsing SHALL fail verification and the program SHALL not start sorting

### Requirement: Optional Tuning Flags
The CLI SHALL accept optional `--blocksize`, `--threads`, `--readbuffersize`, and `--action` flags.

#### Scenario: Defaults when flags omitted
- **WHEN** only inputs and `--output` are provided
- **THEN** the system SHALL use `blocksize=1000000000`, `threads=12`, `readbuffersize=20000000`, and `action=sort`

#### Scenario: Explicit overrides
- **WHEN** the user supplies `--blocksize`, `--threads`, `--readbuffersize`, and/or `--action`
- **THEN** those values SHALL override the corresponding defaults for the run

### Requirement: Full Sort Action
When `action` is `sort` (the default), the system SHALL run external batch sorting of all inputs, then k-way merge of the produced run files into `--output`, then delete the temporary run files.

#### Scenario: End-to-end sort
- **WHEN** `--action sort` (or omitted action) is used with one or more input files
- **THEN** the final `--output` file SHALL contain all input records in record-format order and temporary `*.{batchIndex}` run files SHALL be removed

### Requirement: Non-Sort Action Is Merge-Only
When `action` is any value other than `sort`, the system SHALL skip batch sorting and merge the trailing input files directly into `--output`.

#### Scenario: Merge existing runs
- **WHEN** `--action merge` (or any non-`sort` value) is provided with sorted run paths
- **THEN** the system SHALL merge those paths into `--output` without partitioning them into new batches

### Requirement: No Glob Or Comma-List Input Flag
The CLI SHALL take explicit file paths as trailing arguments and SHALL NOT implement an `--input` flag, comma-separated path lists, or shell-glob expansion inside the program.

#### Scenario: Explicit paths only
- **WHEN** the user needs multiple inputs
- **THEN** each path SHALL be passed as a separate trailing argument (shell globbing, if any, happens outside the program)
