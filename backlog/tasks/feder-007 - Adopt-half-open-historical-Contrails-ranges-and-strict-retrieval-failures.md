---
id: FEDER-007
title: Adopt half-open historical Contrails ranges and strict retrieval failures
status: In Progress
assignee:
  - '@myself'
created_date: '2026-07-20 20:17'
updated_date: '2026-07-20 20:19'
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
- [ ] #1 Contrails historical processing interprets start and end as a half-open interval [start, end).
- [ ] #2 Contrails historical start and end values are interpreted as UTC and rejected unless they are aligned to whole hours.
- [ ] #3 Every hourly file in the requested interval must be retrieved and processed before the run can succeed.
- [ ] #4 Authentication and invalid-request failures fail immediately; 404, 429, server, timeout, and connection failures are retried up to five attempts at approximately five-minute intervals, honoring a bounded Retry-After value when present.
- [ ] #5 Exhausted retrieval retries cause a non-zero receiver exit and cannot emit a successful truncated completion.
- [ ] #6 Existing receiver documentation and automated tests reflect the new half-open and strict-failure behavior.
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
