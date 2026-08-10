## Why

sort16 has baseline OpenSpec requirements but zero automated tests, so regressions in record ordering, external sort, merge, and CLI defaults go undetected. Now that the behavior is locked in specs, we need an in-memory and file-based suite that covers as much of that baseline as practical.

## What Changes

- Add ScalaTest (and existing ZIO Test deps as needed) suites under `src/test/scala/sort16/`
- Add shared test helpers to pack/write/read 16-byte records and assert sort/merge outcomes
- Cover in-memory: record comparison (all key positions, equality, signed ints) and CLI `Conf` defaults / required `--output`
- Cover file-based: single- and multi-block external sort, multi-file inputs, source unchanged, run naming, merge-only mode, merge buffer refill, record preservation, temp-run cleanup, invalid length failure
- Minimal production refactor only if required for testability (e.g. shared compare helper); no intentional product behavior changes
- Out of scope: process-level `sbt run` CLI smoke, GB-scale performance, concurrency stress

## Capabilities

### New Capabilities
- `testing`: Automated in-memory and file-based verification of baseline sort16 behavior across record-format, external-sort, k-way-merge, and cli scenarios

### Modified Capabilities
- (none — product requirements stay as baselined; this change adds verification, not new sorter behavior)

## Impact

- New files under `src/test/scala/sort16/` (suites + helpers)
- Possible small extract in `Sort.scala` (compare helper) if needed to avoid duplicating ordering logic in tests
- Uses existing test deps in `build.sbt` (ScalaTest, ZIO Test); no new runtime dependencies expected
- `sbt test` becomes the primary regression gate for the four baseline specs
