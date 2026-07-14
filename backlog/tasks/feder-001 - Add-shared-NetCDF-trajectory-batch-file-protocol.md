---
id: FEDER-001
title: Add shared NetCDF trajectory-batch file protocol
status: Done
assignee:
  - '@pi'
created_date: '2026-07-13 21:31'
updated_date: '2026-07-14 08:28'
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
- [x] #1 NetCDF read/write helpers live in shared feder_server code and are used by both receiver and ingester apps
- [x] #2 Files use CF contiguous ragged-array representation with trajectory and obs dimensions plus row_size
- [x] #3 Schema preserves TrajectoryBatch semantics, including per-trajectory DataSource and trajectory metadata/points, but does not include end-of-day markers or partial
- [x] #4 Nullable NetCDF values use NetCDF conventions and round-trip back to Feder None values
- [x] #5 Lightweight global attributes include CF conventions, Feder file type, Feder file version, and source/debug metadata
- [x] #6 Unsupported or invalid file versions/schemas are rejected with clear errors
- [x] #7 Semantic round-trip tests cover multiple trajectories, ragged point counts, nullable metadata, nullable numeric point values, and invalid schema/version cases
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

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Implemented shared feder_server.netcdf helpers for writing/reading TrajectoryBatch NetCDF files. Added netcdf4 dependency and uv.lock update. Added semantic round-trip tests covering ragged multi-trajectory batches, nullable trajectory metadata, nullable numeric point values, CF/global attrs, EOD rejection, unsupported versions, and invalid row_size schema.

Validation run:
- uv run pytest tests/server_lib/test_messages.py tests/server_lib/test_netcdf.py -q
- uv run pytest tests/server_lib/test_config_paths.py tests/server_lib/test_messages.py tests/server_lib/test_netcdf.py tests/common_lib tests/feder_rx/test_processor.py tests/feder_rx/test_commands.py tests/feder_ingest/test_db_cache_paths.py -q
- uv run ruff check libs/server/src/feder_server/netcdf.py tests/server_lib/test_netcdf.py
- uv run pyright libs/server/src/feder_server/netcdf.py

Note: full tests/server_lib includes RabbitMQ integration tests that fail in this environment because no local RabbitMQ broker is listening on localhost:5672.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Added the shared Feder NetCDF trajectory-batch protocol implementation.

Changes:
- Added feder_server.netcdf with versioned read/write helpers for TrajectoryBatch payloads using CF contiguous ragged trajectory arrays (trajectory/obs dimensions plus row_size).
- Encoded per-trajectory DataSource and metadata plus point observations, omitting RabbitMQ-only end-of-day and partial semantics.
- Added lightweight global Feder/CF attributes and clear validation errors for unsupported versions and malformed schemas.
- Added netcdf4 to feder-server dependencies and exported the helpers from feder_server.
- Added semantic round-trip and invalid-file tests.

Tests:
- uv run pytest tests/server_lib/test_messages.py tests/server_lib/test_netcdf.py -q
- uv run pytest tests/server_lib/test_config_paths.py tests/server_lib/test_messages.py tests/server_lib/test_netcdf.py tests/common_lib tests/feder_rx/test_processor.py tests/feder_rx/test_commands.py tests/feder_ingest/test_db_cache_paths.py -q
- uv run ruff check libs/server/src/feder_server/netcdf.py tests/server_lib/test_netcdf.py
- uv run pyright libs/server/src/feder_server/netcdf.py

Caveat:
- Full tests/server_lib was not fully runnable here because RabbitMQ integration tests require a local broker on localhost:5672.
<!-- SECTION:FINAL_SUMMARY:END -->
