---
id: FEDER-007
title: Adopt half-open historical Contrails ranges and strict retrieval failures
status: Done
assignee:
  - '@myself'
created_date: '2026-07-20 20:17'
updated_date: '2026-07-21 05:10'
labels:
  - receiver
  - contrails
  - file-workflow
dependencies: []
priority: high
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Establish the range and failure semantics required by cursor-managed scheduled downloads. Historical Contrails processing must use UTC hourly half-open intervals and must never report success after retrieving only part of a requested interval.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Contrails historical processing interprets start and end as a half-open interval [start, end).
- [x] #2 Contrails historical start and end values are interpreted as UTC and rejected unless they are aligned to whole hours.
- [x] #3 Every hourly file in the requested interval must be retrieved and processed before the run can succeed.
- [x] #4 Authentication and invalid-request failures fail immediately; 404, 429, server, timeout, and connection failures are retried up to five attempts at approximately five-minute intervals, honoring a bounded Retry-After value when present.
- [x] #5 Exhausted retrieval retries cause a non-zero receiver exit and cannot emit a successful truncated completion.
- [x] #6 Existing receiver documentation and automated tests reflect the new half-open and strict-failure behavior.
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Inspect the historical DateSource/Contrails source-to-processor completion path and existing CLI tests to identify every place that currently assumes inclusive or rounded endpoints.
2. Introduce explicit whole-hour validation for Contrails historical boundaries while retaining Feder's convention that parsed timestamps are UTC, and change historical iteration to process [start, end).
3. Refactor Contrails retrieval failures into an explicit success/failure path so historical source completion cannot be emitted after a truncated interval.
4. Add bounded retry handling for retryable HTTP/network failures, including capped Retry-After support, while keeping authentication and invalid-request failures immediate and preserving appropriate live-mode behavior.
5. Add focused source and CLI regression tests for endpoint exclusion, alignment rejection, complete intervals, retry classification/exhaustion, and non-zero truncated-run exits.
6. Update receiver documentation for half-open manual ranges, then run the receiver test suite and project lint/type checks relevant to the touched modules.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
- Implemented UTC-aware ISO parsing and Contrails whole-hour validation with half-open historical iteration.
- Added explicit retrieval failures, five-attempt retry classification, bounded delta/date Retry-After support, request timeouts, and terminal exhaustion propagation through Processor to receiver exit status.
- Added strict failure, range, timezone, retry, and file-output CLI regression tests; updated receiver documentation.
- Validation: `uv run pytest -q tests/feder_rx` (27 passed), `uv run ruff check apps/feder-rx/src/feder_rx tests/feder_rx/test_contrails_api.py tests/feder_rx/test_file_output_mode.py`, and `uv run pyright apps/feder-rx/src/feder_rx/sources/contrails_api.py apps/feder-rx/src/feder_rx/processor.py apps/feder-rx/src/feder_rx/__init__.py`.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Adopted strict UTC hourly Contrails historical ranges and retrieval failure semantics.

Changes:
- Treat historical ranges as half-open `[start, end)` intervals and reject non-hour-aligned boundaries.
- Normalize CLI timestamps to UTC, including explicit offsets.
- Retry missing, rate-limited, timeout, connection, server, and malformed-response failures up to five attempts with bounded Retry-After handling.
- Propagate terminal source failures through Processor so historical/file-output runs exit non-zero rather than reporting truncated success.
- Updated receiver documentation and added focused source/CLI regression tests.

Tests:
- `uv run pytest -q tests/feder_rx` (27 passed)
- `uv run ruff check apps/feder-rx/src/feder_rx tests/feder_rx/test_contrails_api.py tests/feder_rx/test_file_output_mode.py`
- `uv run pyright apps/feder-rx/src/feder_rx/sources/contrails_api.py apps/feder-rx/src/feder_rx/processor.py apps/feder-rx/src/feder_rx/__init__.py`
<!-- SECTION:FINAL_SUMMARY:END -->
