---
id: FEDER-007
title: Adopt half-open historical Contrails ranges and strict retrieval failures
status: To Do
assignee: []
created_date: '2026-07-20 20:17'
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
