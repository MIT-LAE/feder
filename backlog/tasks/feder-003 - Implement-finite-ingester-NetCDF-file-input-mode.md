---
id: FEDER-003
title: Implement finite ingester NetCDF file-input mode
status: To Do
assignee: []
created_date: '2026-07-13 21:31'
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
