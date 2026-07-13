---
id: FEDER-001
title: Add shared NetCDF trajectory-batch file protocol
status: To Do
assignee: []
created_date: '2026-07-13 21:31'
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
