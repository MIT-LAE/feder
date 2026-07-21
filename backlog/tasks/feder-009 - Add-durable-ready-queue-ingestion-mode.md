---
id: FEDER-009
title: Add durable ready-queue ingestion mode
status: Done
assignee:
  - '@myself'
created_date: '2026-07-20 20:17'
updated_date: '2026-07-21 07:09'
labels:
  - ingester
  - scheduling
  - file-workflow
  - reliability
dependencies:
  - FEDER-008
references:
  - apps/feder-ingest/src/feder_ingest/__init__.py
  - apps/feder-ingest/src/feder_ingest/db_cache.py
  - apps/feder-ingest/src/feder_ingest/processor.py
documentation:
  - apps/feder-ingest/README.md
priority: high
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Extend finite file ingestion to drain completed scheduled receiver runs safely. Each run directory becomes the ingestion commit unit so abrupt Slurm termination cannot delete the only durable input before database publication.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 feder-ingest --file-input-queue derives the ready directory from receiver.queue-directory and is mutually exclusive with --file-input-directory.
- [x] #2 Queue mode remains finite and file-only: it constructs neither RabbitMQ nor Prometheus services and takes a single snapshot of ready directories at startup.
- [x] #3 Ready run directories are validated against the scheduled naming convention and processed oldest interval first; unexpected visible top-level entries cause a non-zero exit.
- [x] #4 Within each ready run directory, queue mode accepts only visible regular *.nc files; an unexpected visible entry fails that run, while an empty valid run is accepted.
- [x] #5 All NetCDF files in one run are processed without deleting any input, then DBCache.force_publish succeeds before that run directory and its files are removed.
- [x] #6 Publication, decoding, validation, or processing failure retains the complete run directory, stops queue processing immediately, and leaves later runs untouched.
- [x] #7 Any individual trajectory insertion failure is fatal in finite queue mode and retains the input run, while live RabbitMQ ingestion retains its existing log-and-continue behavior.
- [x] #8 If queue ingestion is interrupted before publication and deletion, the retained run can be retried safely even when staging contains partial or duplicate data.
- [x] #9 An empty ready queue still calls DBCache.force_publish so dirty durable staging from an earlier failed publication is retried.
- [x] #10 Automated tests cover fixed-snapshot and oldest-first behavior, strict entry validation, empty runs and queues, insertion and publication failures, process-publish-delete ordering, retained inputs, stopping at the first failure, and successful directory removal.
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Add the mutually exclusive --file-input-queue CLI mode and resolve its ready path from receiver.queue-directory under the same file-only configuration and path-safety rules.
2. Define and test scheduled run-name parsing/ordering plus strict queue snapshots: ignore only permitted hidden temporary entries, reject unexpected visible top-level entries, and validate each run directory contains only regular visible NetCDF files.
3. Add an explicit strict finite-ingestion policy to Processor so any trajectory insertion error propagates in file modes while live RabbitMQ processing retains log-and-continue behavior.
4. Refactor file ingestion so queue mode creates one DBCache/Processor, snapshots ready directories once, and processes them oldest first without deleting inputs during decoding or database mutation.
5. For each run, force-publish all pending database changes before deleting its files and directory; on any read, insert, or publish failure retain the run and stop before later directories. Treat empty run directories as valid commit units.
6. Ensure an empty queue still force-publishes recovered dirty staging, and verify interruption/retry semantics remain duplicate-safe with partially updated staging.
7. Add CLI and integration tests for ordering, fixed snapshots, strict validation, insertion failures, publication failures, process-publish-delete ordering, retained inputs, empty runs/queues, and successful cleanup; run ingester tests and relevant lint/type checks.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
- Implemented durable scheduled ready-queue ingestion with strict validation, per-run publish barrier, and input retention on failure.
- Added queue-mode CLI/integration coverage and operator documentation.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Implemented durable ready-queue ingestion for FEDER-009.

Changes:
- Added finite, file-only `--file-input-queue` mode with receiver queue configuration validation, startup snapshots, scheduled-run ordering, and strict run contents.
- Made ready runs publish-before-delete commit units; failures retain the run and halt processing.
- Added strict insertion behavior for queue mode while preserving RabbitMQ and legacy file-directory behavior.
- Documented scheduled queue operations and added integration coverage.

Tests:
- `uv run pytest tests/feder_ingest/test_file_input_mode.py -q`
- `uv run ruff check apps/feder-ingest/src/feder_ingest libs/server/src/feder_server tests/feder_ingest/test_file_input_mode.py`
- Full pytest was attempted: 116 passed, 1 skipped; 4 RabbitMQ integration tests could not start because no broker was listening on localhost:5672.
<!-- SECTION:FINAL_SUMMARY:END -->
