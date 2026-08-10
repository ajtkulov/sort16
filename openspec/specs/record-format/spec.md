## Purpose

Define the fixed-width binary record layout and total order used by sort16 when comparing and writing records.

## Requirements

### Requirement: Fixed 16-Byte Records
The system SHALL treat every input and output file as a sequence of fixed-length 16-byte records with no headers, footers, or delimiters.

#### Scenario: Valid file length
- **WHEN** a file is read for sorting or merging
- **THEN** the number of bytes read in each I/O operation MUST be divisible by 16

#### Scenario: Invalid trailing bytes
- **WHEN** a read returns a byte count that is not divisible by 16
- **THEN** the system SHALL fail (assertion failure) rather than process a partial record

### Requirement: Big-Endian Four-Int Key Order
The system SHALL compare records as four signed 32-bit big-endian integers occupying bytes `[0..3]`, `[4..7]`, `[8..11]`, and `[12..15]`, in that order.

#### Scenario: First differing integer decides order
- **WHEN** two records differ at integer index `i` (0-based) and agree on all integers before `i`
- **THEN** the record with the smaller signed integer at index `i` SHALL be ordered before the other

#### Scenario: All four integers equal
- **WHEN** two records have identical four integers
- **THEN** the comparison SHALL treat them as equal (neither strictly less than the other)

#### Scenario: Signed interpretation
- **WHEN** a record's integer bytes represent a value with the high bit set (for example `0xFFFFFFFF`)
- **THEN** that integer SHALL be compared as a signed 32-bit value (negative), not as unsigned
