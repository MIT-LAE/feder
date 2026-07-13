---
id: FEDER-004
title: Implement receiver file-output mode with trajectory sink abstraction
status: To Do
assignee: []
created_date: '2026-07-13 21:32'
updated_date: '2026-07-13 21:33'
labels:
  - file-mode
  - receiver
  - netcdf
dependencies: []
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Add --file-output-directory to feder-rx. In this historical-only mode, the receiver should avoid all RabbitMQ and ingester-liveness behavior, use the existing source-thread and processor queue architecture, and publish completed trajectory batches as atomic NetCDF files for later ingester consumption.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 --file-output-directory implies file-output mode and no RMQ or ingester-liveness objects are constructed or started
- [ ] #2 File-output mode requires both --start-time and --end-time and rejects live mode and unsupported file/glob-only sources for v1
- [ ] #3 Receiver output directory is created if missing, must be a directory, must not overlap configured data/staging/scratch roots, and must contain no visible *.nc files at startup
- [ ] #4 Receiver processor uses a trajectory sink abstraction so RabbitMQ and NetCDF file delivery are isolated
- [ ] #5 NetCDF output files are named <receiver-name>.<sequence:08d>.nc and written via hidden temp file plus atomic rename in lexicographic sequence
- [ ] #6 A successful atomic NetCDF publish is the ACK boundary: corresponding receiver staging fixes are deleted only after publish succeeds
- [ ] #7 End-of-day and ingester backpressure/liveness behavior are disabled in file-output mode
- [ ] #8 File-output mode keeps current historical in-memory staging behavior and treats non-empty staging after final completion as an error
- [ ] #9 Tests cover historical validation, no-RMQ behavior, atomic publish/delete boundary, output directory validation, and non-empty final staging failure
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Introduce a receiver trajectory sink abstraction that accepts TrajectoryBatch plus source IDs and isolates RabbitMQ-specific ACK/NACK behavior from file publishing.
2. Adapt existing RabbitMQ receiver path to use the sink while preserving current pending message and delete-on-ACK semantics.
3. Implement a NetCDF file sink that writes batches to hidden temp files, fsyncs/closes them, atomically renames to <receiver-name>.<sequence:08d>.nc, fsyncs the directory best-effort, and deletes staging fixes only after successful publish.
4. Add --file-output-directory CLI option and mode branch that skips RMQ construction, ingester liveness, and backpressure/status handling entirely.
5. Validate file-output mode: require --start-time and --end-time, reject live mode and unsupported glob/file-only sources for v1, create/validate output directory, reject existing visible *.nc files, and validate path-root non-overlap.
6. Preserve current historical in-memory receiver staging and source-thread/queue/processor architecture.
7. Disable end-of-day sending in file-output mode and fail the run if final receiver staging is non-empty after final completion drains.
8. Add tests for no-RMQ behavior, historical argument validation, output directory validation, atomic publish/delete boundary, write failure preserving staging, and non-empty final staging failure.
9. Run focused receiver tests and existing RabbitMQ-mode regression tests.
<!-- SECTION:PLAN:END -->
