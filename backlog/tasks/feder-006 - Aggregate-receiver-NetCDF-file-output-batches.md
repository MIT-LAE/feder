---
id: FEDER-006
title: Aggregate receiver NetCDF file-output batches
status: Done
assignee:
  - '@pi'
created_date: '2026-07-14 14:17'
updated_date: '2026-07-14 14:21'
labels: []
dependencies: []
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Aggregate multiple receiver trajectory batches into larger NetCDF files in file-output mode so historical Contrails API receiver runs do not produce thousands of tiny files. Add a CLI sizing knob while preserving deterministic ordering, atomic publication, final flush behavior, and clear failure semantics.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 feder-rx exposes --file-output-max-trajectories with default 10000 and validates it is at least 1
- [x] #2 File-output sink buffers trajectory batches and publishes an aggregate NetCDF file when buffered trajectories reach or exceed the configured threshold
- [x] #3 File-output sink flushes any remaining buffered trajectories before finite receiver shutdown
- [x] #4 Aggregate files preserve trajectory ordering and carry the latest cumulative trajectory_count from included batches
- [x] #5 For file-output aggregation, receiver staging rows for buffered trajectories are deleted after successful materialization into the sink buffer
- [x] #6 Tests cover aggregation threshold behavior, final partial flush, CLI option wiring, and existing atomic failure behavior
- [x] #7 Receiver documentation describes aggregation, default sizing, final flush, and failure/rerun semantics
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Add a TrajectorySink finalization hook and wire Processor shutdown to call it exactly once after queued trajectories are drained but before file-output mode verifies empty staging. Keep RabbitMQ finalization as a no-op.
2. Refactor NetCDFFileTrajectorySink to buffer materialized Trajectory objects, source IDs, and the latest cumulative trajectory_count. On publish_trajectories, append the incoming batch, delete corresponding staging rows after successful buffering, and flush when buffered trajectory count reaches or exceeds max_trajectories.
3. Preserve existing atomic NetCDF publish logic by moving it into a private _publish_buffer/_write_batch helper that writes hidden temp files, fsyncs best-effort, atomically renames, fsyncs the directory, increments sequence numbers, and cleans temp files on failure.
4. Add --file-output-max-trajectories to feder-rx as a click.IntRange(min=1) option with default 10000, and pass it into NetCDFFileTrajectorySink only for file-output mode.
5. Update tests in tests/feder_rx/test_file_output_mode.py to cover threshold aggregation, final partial flush, latest cumulative trajectory_count, staging deletion after buffering, CLI option wiring/validation, and that atomic write failure still leaves no visible corrupt file.
6. Update apps/feder-rx/README.md to document aggregation, default sizing, final flush, and failure/rerun semantics for successful vs failed receiver runs.
7. Run focused receiver and NetCDF tests, then mark acceptance criteria complete and add implementation notes/final summary if implementation proceeds.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
- Implemented NetCDF file-output aggregation in the sink with a finalization hook called by Processor shutdown.
- Added --file-output-max-trajectories (default 10000, min 1) and passed it to file-output sink construction.
- Updated tests for threshold aggregation, final flush through processor shutdown, CLI validation/wiring, and atomic failure/no-visible-file behavior.
- Updated receiver README with aggregation, final flush, and rerun-on-failure semantics.
- Tests: uv run pytest tests/feder_rx/test_file_output_mode.py tests/server_lib/test_netcdf.py -q; uv run pytest tests/feder_rx -q
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Aggregated receiver NetCDF file-output batches so historical file-only receiver runs produce larger files instead of one small file per processor batch.

Changes:
- Added --file-output-max-trajectories with default 10000 and Click min-value validation.
- Added a TrajectorySink finalization hook and made Processor flush sink-owned buffers before finite shutdown completes.
- Refactored NetCDFFileTrajectorySink to buffer materialized trajectories, preserve ordering, carry the latest cumulative trajectory_count, publish aggregate files atomically, and delete staging rows after successful buffering.
- Documented aggregation sizing, final partial flush, and failed-run rerun semantics.

Tests:
- uv run pytest tests/feder_rx/test_file_output_mode.py tests/server_lib/test_netcdf.py -q
- uv run pytest tests/feder_rx -q
<!-- SECTION:FINAL_SUMMARY:END -->
