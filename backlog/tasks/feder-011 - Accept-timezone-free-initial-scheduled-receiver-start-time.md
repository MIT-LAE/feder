---
id: FEDER-011
title: Accept timezone-free initial scheduled receiver start time
status: To Do
assignee: []
created_date: '2026-07-21 11:33'
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
- [ ] #1 feder-rx-scheduled accepts a timezone-free --initial-start-time value and interprets it as UTC
- [ ] #2 Whole-hour validation remains enforced and invalid timestamp forms still fail clearly
- [ ] #3 Scheduled receiver tests cover the timezone-free CLI input convention
<!-- AC:END -->
