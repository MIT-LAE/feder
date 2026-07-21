---
id: FEDER-008
title: Add cursor-managed scheduled Contrails receiver runs
status: Done
assignee:
  - '@myself'
created_date: '2026-07-20 20:17'
updated_date: '2026-07-21 06:56'
labels:
  - receiver
  - contrails
  - scheduling
  - file-workflow
dependencies:
  - FEDER-007
references:
  - apps/feder-rx/src/feder_rx/__init__.py
  - apps/feder-rx/src/feder_rx/sources/contrails_api.py
documentation:
  - apps/feder-rx/README.md
priority: high
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Add a finite application-level receiver command for recurring cluster execution. The command selects one bounded interval from durable cursor state, publishes a complete run into a ready queue, and advances state only after durable output publication.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 A feder-rx-scheduled command accepts a required source argument, supports contrails-api, and fails clearly for unsupported sources.
- [x] #2 Receiver configuration provides a queue directory and a configurable maximum run duration defaulting to 24 hours; the queue root is validated not to overlap configured data, staging, or scratch roots.
- [x] #3 When cursor state is absent, an explicit whole-hour UTC initial start is required; a valid initial cursor is persisted before the first download, while a malformed existing cursor fails without reinitialization.
- [x] #4 cursor.json contains version, source, and next_time fields and is updated by flushed temporary-file plus atomic rename.
- [x] #5 The availability cutoff is the whole-hour floor of current UTC time minus the configured source data lag, and the selected interval end is min(cursor + maximum duration, cutoff).
- [x] #6 A cursor equal to the cutoff exits successfully without creating a run; a cursor ahead of the cutoff logs a warning and exits successfully without moving backward.
- [x] #7 Each run writes to a unique incomplete directory named with its UTC [start, end) interval, source, and unique suffix, then atomically renames that directory into ready only after every requested hour succeeds.
- [x] #8 The cursor advances to the exclusive interval end only after the ready-directory rename succeeds; failures never advance it, so a crash window may cause duplicates but cannot cause a gap.
- [x] #9 Handled failures remove their incomplete directory, while abruptly abandoned incomplete directories are neither reused nor automatically deleted.
- [x] #10 A successfully retrieved and processed interval with no output NetCDF files is still published as an empty ready directory and advances the cursor.
- [x] #11 Automated tests cover bootstrap, corrupt state, cutoff flooring, chunk selection, no-work and ahead-of-cutoff behavior, unsupported sources, directory lifecycle, failure ordering, atomic cursor updates, and empty successful runs.
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Extend server configuration parsing and file-only requirements with receiver.queue-directory and receiver.max-run-duration, including the 24-hour default, positive whole-hour validation, and path-root isolation checks.
2. Add the feder-rx-scheduled entry point and a small scheduled-run module that validates the required source argument, supports Contrails only, and exposes pure helpers for whole-hour cutoff and bounded interval selection.
3. Implement versioned cursor loading, bootstrap validation, and atomic cursor persistence using a flushed temporary file and os.replace; distinguish absent state from malformed state and handle equal/ahead cutoffs without moving backward.
4. Refactor the finite receiver execution path into reusable application code so scheduled mode can run one explicit [start, end) Contrails interval without constructing RabbitMQ or Prometheus services.
5. Create sortable unique run names, write into queue/incomplete, remove incomplete output on handled failure, atomically rename successful runs into queue/ready, and only then advance the cursor. Preserve empty successful output directories.
6. Add unit and CLI tests with injected time and mocked receiver execution for bootstrap, cursor validation, cutoff flooring, 24-hour bounding, no-work cases, unsupported sources, failure cleanup, publication ordering, cursor write failures, and empty runs.
7. Update configuration examples and receiver documentation for the command and queue layout, then run the receiver/server tests and relevant lint/type checks.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
- Implemented durable scheduled Contrails cursor, isolated queue configuration, atomic ready publication, and tests.
- Verified with `uv run ruff check ...` and full `uv run pytest` (114 passed, 1 skipped; RabbitMQ test service started locally).
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Implemented FEDER-008 scheduled Contrails receiver runs.

Changes:
- Added `feder-rx-scheduled` with durable versioned cursor bootstrap/update, hourly availability cutoff, bounded intervals, and safe incomplete-to-ready publication ordering.
- Added required scheduled queue configuration with root isolation and 24-hour default positive whole-hour run duration.
- Documented queue operation and added cursor, lifecycle, failure-ordering, config, and CLI coverage.

Tests:
- `uv run ruff check apps/feder-rx/src/feder_rx/scheduled.py tests/feder_rx/test_scheduled.py libs/server/src/feder_server/config.py tests/server_lib/test_config_paths.py`
- `uv run pytest` (114 passed, 1 skipped)
<!-- SECTION:FINAL_SUMMARY:END -->
