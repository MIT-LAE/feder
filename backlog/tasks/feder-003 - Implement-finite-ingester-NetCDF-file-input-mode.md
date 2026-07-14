---
id: FEDER-003
title: Implement finite ingester NetCDF file-input mode
status: In Progress
assignee:
  - '@pi'
created_date: '2026-07-13 21:31'
updated_date: '2026-07-14 10:31'
labels:
  - file-mode
  - ingester
  - netcdf
dependencies: []
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Add --file-input-directory to feder-ingest. In this finite batch mode, the ingester must avoid all RabbitMQ control flow, read visible NetCDF trajectory-batch files in deterministic order, ingest them with the same per-batch semantics as RabbitMQ mode, delete successfully processed files, force-publish all potentially stale databases, and exit.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 --file-input-directory implies finite file-input mode and no RMQ objects are constructed or started
- [ ] #2 Input directory must already exist, must be a directory, and must not overlap configured data, staging, or scratch roots
- [ ] #3 Visible *.nc files are processed in lexicographic order; hidden files are ignored; visible non-NetCDF entries are warned about and ignored
- [ ] #4 TrajectoryBatch handling is refactored into a shared processor method used by both RabbitMQ and file-input paths without fake RMQ wrappers
- [ ] #5 Unreadable or invalid NetCDF files cause nonzero exit, are left in place, and prevent later files from being processed
- [ ] #6 Valid files are deleted after all contained trajectories are attempted, even if individual trajectory database inserts fail and are logged like RabbitMQ mode
- [ ] #7 After input files are consumed, the ingester commits, promotes, exports, and publishes all dirty/touched databases regardless of export interval
- [ ] #8 An empty input directory is a successful no-op that still attempts final dirty staging publish
- [ ] #9 Tests cover happy path deletion/publication, invalid file retention, empty-directory publish retry, and final-publish failure/retry behavior
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Refactor feder_ingest.processor so RabbitMQ DataMessage handling delegates to a process_trajectory_batch method with unchanged per-trajectory error behavior.
2. Add DBCache support for forcing final publish of all dirty/touched staged and nursery databases on demand, including empty-input retry after previous publish failure.
3. Add --file-input-directory CLI option and branch startup before any RMQ or Prometheus-only setup.
4. Validate input directory existence/type and non-overlap with configured data, staging, and scratch roots.
5. Implement finite file consumer: list visible *.nc files lexicographically, warn/ignore visible non-NetCDF entries, ignore hidden entries, read each batch, process it, and delete the file only after successful file-level processing.
6. Handle invalid/unreadable NetCDF by logging, leaving the file in place, stopping before later files, and exiting nonzero.
7. Ensure final forced publish runs after all files are consumed, including when the directory starts empty.
8. Add tests for happy path, invalid file retention, empty-directory publish retry, and final-publish failure/retry semantics.
9. Run focused ingester tests and relevant existing regression tests.
<!-- SECTION:PLAN:END -->
