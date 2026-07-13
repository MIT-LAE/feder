---
id: FEDER-001
title: Add shared NetCDF trajectory-batch file protocol
status: To Do
assignee: []
created_date: '2026-07-13 21:31'
updated_date: '2026-07-13 21:33'
labels:
  - file-mode
  - netcdf
  - protocol
dependencies: []
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Define and implement the versioned Feder NetCDF trajectory-batch interchange used by receiver file-output mode and ingester file-input mode. The format should encode RabbitMQ-equivalent TrajectoryBatch payloads as CF-compliant contiguous ragged trajectory arrays using netCDF4, with lightweight Feder validation attributes.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 NetCDF read/write helpers live in shared feder_server code and are used by both receiver and ingester apps
- [ ] #2 Files use CF contiguous ragged-array representation with trajectory and obs dimensions plus row_size
- [ ] #3 Schema preserves TrajectoryBatch semantics, including per-trajectory DataSource and trajectory metadata/points, but does not include end-of-day markers or partial
- [ ] #4 Nullable NetCDF values use NetCDF conventions and round-trip back to Feder None values
- [ ] #5 Lightweight global attributes include CF conventions, Feder file type, Feder file version, and source/debug metadata
- [ ] #6 Unsupported or invalid file versions/schemas are rejected with clear errors
- [ ] #7 Semantic round-trip tests cover multiple trajectories, ragged point counts, nullable metadata, nullable numeric point values, and invalid schema/version cases
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Inspect existing TrajectoryBatch, Trajectory, Point, and DataSource models plus existing serialization tests to match current RabbitMQ semantics.
2. Add netCDF4 dependency to shared/server package and app dependency graph as needed.
3. Implement feder_server NetCDF helpers for writing and reading TrajectoryBatch objects using CF contiguous ragged arrays.
4. Define lightweight validation constants for Feder file type/version and required dimensions, variables, and global attributes.
5. Encode nullable metadata and point fields using NetCDF conventions, converting cleanly back to None on read.
6. Add semantic round-trip tests for multi-trajectory ragged batches and invalid schema/version tests.
7. Run focused tests for the shared protocol and update exports/imports if callers need a stable public API.
<!-- SECTION:PLAN:END -->
