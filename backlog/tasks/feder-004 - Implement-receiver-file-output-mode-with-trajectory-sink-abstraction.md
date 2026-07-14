---
id: FEDER-004
title: Implement receiver file-output mode with trajectory sink abstraction
status: Done
assignee:
  - '@pi'
created_date: '2026-07-13 21:32'
updated_date: '2026-07-14 13:35'
labels:
  - file-mode
  - receiver
  - netcdf
dependencies: []
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Add --file-output-directory to feder-rx. In this historical-only mode, the receiver should avoid all RabbitMQ and ingester-liveness behavior, use the existing source-thread and processor queue architecture, and publish completed trajectory batches as atomic NetCDF files for later ingester consumption.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 --file-output-directory implies file-output mode and no RMQ or ingester-liveness objects are constructed or started
- [x] #2 File-output mode requires both --start-time and --end-time and rejects live mode and unsupported file/glob-only sources for v1
- [x] #3 Receiver output directory is created if missing, must be a directory, must not overlap configured data/staging/scratch roots, and must contain no visible *.nc files at startup
- [x] #4 Receiver processor uses a trajectory sink abstraction so RabbitMQ and NetCDF file delivery are isolated
- [x] #5 NetCDF output files are named <receiver-name>.<sequence:08d>.nc and written via hidden temp file plus atomic rename in lexicographic sequence
- [x] #6 A successful atomic NetCDF publish is the ACK boundary: corresponding receiver staging fixes are deleted only after publish succeeds
- [x] #7 End-of-day and ingester backpressure/liveness behavior are disabled in file-output mode
- [x] #8 File-output mode keeps current historical in-memory staging behavior and treats non-empty staging after final completion as an error
- [x] #9 Tests cover historical validation, no-RMQ behavior, atomic publish/delete boundary, output directory validation, and non-empty final staging failure
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Introduce a receiver trajectory sink abstraction that accepts TrajectoryBatch plus source IDs and isolates RabbitMQ-specific ACK/NACK behavior from file publishing.
2. Adapt existing RabbitMQ receiver path to use the sink while preserving current pending message and delete-on-ACK semantics.
3. Implement a NetCDF file sink that writes batches to hidden temp files, fsyncs/closes them, atomically renames to <receiver-name>.<sequence:08d>.nc, fsyncs the directory best-effort, and deletes staging fixes only after successful publish.
4. Add --file-output-directory CLI option and mode branch that skips RMQ construction, ingester liveness, and backpressure/status handling entirely.
5. Validate file-output mode: require --start-time and --end-time, reject live mode and unsupported glob/file-only sources for v1, create/validate output directory, reject existing visible *.nc files, and validate path-root non-overlap.
6. Preserve current historical in-memory receiver staging and source-thread/queue/processor architecture.
7. Disable end-of-day sending in file-output mode and fail the run if final receiver staging is non-empty after final completion drains.
8. Add tests for no-RMQ behavior, historical argument validation, output directory validation, atomic publish/delete boundary, write failure preserving staging, and non-empty final staging failure.
9. Run focused receiver tests and existing RabbitMQ-mode regression tests.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Implemented receiver file-output mode with a TrajectorySink abstraction (RabbitMQ sink plus atomic NetCDF file sink), CLI validation, no-RMQ/no-liveness file-output branch, and processor final-drain handling for synchronous file publishes.

Validation run:
- uv run ruff check apps/feder-rx/src/feder_rx/processor.py apps/feder-rx/src/feder_rx/sinks.py apps/feder-rx/src/feder_rx/__init__.py tests/feder_rx/test_file_output_mode.py
- uv run pyright apps/feder-rx/src/feder_rx/processor.py apps/feder-rx/src/feder_rx/sinks.py tests/feder_rx/test_file_output_mode.py
- uv run pytest -q
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Implemented receiver file-output mode for historical date-range sources.

Changes:
- Added --file-output-directory and file-only configuration loading so RabbitMQ and ingester-liveness objects are skipped entirely in file-output mode.
- Added output-directory validation: create missing directories, require directories, reject visible *.nc files, and prevent overlap with data/staging/scratch roots.
- Introduced a TrajectorySink abstraction with RabbitMQ and NetCDF implementations, preserving RabbitMQ ACK/NACK semantics while adding atomic NetCDF publish as the file-mode ACK boundary.
- NetCDF sink writes hidden temp files, best-effort fsyncs, atomically renames to <receiver-name>.<sequence:08d>.nc, then deletes staging fixes only after publish succeeds.
- Processor now handles synchronous sinks without waiting for RMQ ACK messages, suppresses end-of-day delivery for file output, and treats non-empty final staging as a file-output error.
- Added receiver file-output tests covering validation, no-RMQ/no-liveness operation, atomic publish/delete behavior, sequencing, and final staging drain.

Tests:
- uv run ruff check apps/feder-rx/src/feder_rx/processor.py apps/feder-rx/src/feder_rx/sinks.py apps/feder-rx/src/feder_rx/__init__.py tests/feder_rx/test_file_output_mode.py
- uv run pyright apps/feder-rx/src/feder_rx/processor.py apps/feder-rx/src/feder_rx/sinks.py tests/feder_rx/test_file_output_mode.py
- uv run pytest -q
<!-- SECTION:FINAL_SUMMARY:END -->
