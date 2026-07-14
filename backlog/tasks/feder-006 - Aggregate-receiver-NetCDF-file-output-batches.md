---
id: FEDER-006
title: Aggregate receiver NetCDF file-output batches
status: In Progress
assignee:
  - '@pi'
created_date: '2026-07-14 14:17'
updated_date: '2026-07-14 14:17'
labels: []
dependencies: []
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Aggregate multiple receiver trajectory batches into larger NetCDF files in file-output mode so historical Contrails API receiver runs do not produce thousands of tiny files. Add a CLI sizing knob while preserving deterministic ordering, atomic publication, final flush behavior, and clear failure semantics.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 feder-rx exposes --file-output-max-trajectories with default 10000 and validates it is at least 1
- [ ] #2 File-output sink buffers trajectory batches and publishes an aggregate NetCDF file when buffered trajectories reach or exceed the configured threshold
- [ ] #3 File-output sink flushes any remaining buffered trajectories before finite receiver shutdown
- [ ] #4 Aggregate files preserve trajectory ordering and carry the latest cumulative trajectory_count from included batches
- [ ] #5 For file-output aggregation, receiver staging rows for buffered trajectories are deleted after successful materialization into the sink buffer
- [ ] #6 Tests cover aggregation threshold behavior, final partial flush, CLI option wiring, and existing atomic failure behavior
- [ ] #7 Receiver documentation describes aggregation, default sizing, final flush, and failure/rerun semantics
<!-- AC:END -->
