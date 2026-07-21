---
id: FEDER-011
title: Accept timezone-free initial scheduled receiver start time
status: Done
assignee:
  - '@pi'
created_date: '2026-07-21 11:33'
updated_date: '2026-07-21 11:35'
labels:
  - bug
  - contrails
  - receiver
dependencies: []
references:
  - apps/feder-rx/src/feder_rx/scheduled.py
  - tests/feder_rx/test_scheduled.py
priority: high
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Make the scheduled Contrails receiver bootstrap timestamp follow Feder's established time-argument convention: timezone-free values are interpreted as UTC rather than rejected for lacking an explicit offset.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 feder-rx-scheduled accepts a timezone-free --initial-start-time value and interprets it as UTC
- [x] #2 Whole-hour validation remains enforced and invalid timestamp forms still fail clearly
- [x] #3 Scheduled receiver tests cover the timezone-free CLI input convention
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Reproduce the offset-required rejection with a focused scheduled receiver test.
2. Align scheduled timestamp parsing with Feder's UTC-naive input convention while preserving whole-hour validation and UTC-aware internal state.
3. Update CLI coverage and help text as needed.
4. Run the focused receiver tests plus lint/type checks, then self-review and complete the task.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Reproduced from the user CLI invocation: _parse_whole_hour explicitly rejects parsed datetimes without tzinfo.

- Added regression coverage using timezone-free bootstrap timestamps through both run_scheduled and the Click CLI.
- Updated scheduled parsing to attach UTC to naive ISO timestamps while continuing to normalize offset-aware timestamps.
- Preserved whole-hour rejection and clarified CLI help.
- Validation: 22 focused receiver tests passed; Ruff and Pyright passed; independent self-review found no issues.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Fixed feder-rx-scheduled bootstrap timestamps to follow Feder's standard CLI time convention.

Changes:
- Timezone-free --initial-start-time values are now interpreted as UTC.
- Explicit-offset values remain supported and normalized to UTC.
- Whole-hour validation remains enforced.
- CLI help and regression tests now document and exercise timezone-free input.

Validation:
- uv run pytest -q tests/feder_rx/test_scheduled.py tests/feder_rx/test_contrails_api.py (22 passed)
- uv run ruff check apps/feder-rx/src/feder_rx/scheduled.py tests/feder_rx/test_scheduled.py
- uv run pyright apps/feder-rx/src/feder_rx/scheduled.py
<!-- SECTION:FINAL_SUMMARY:END -->
