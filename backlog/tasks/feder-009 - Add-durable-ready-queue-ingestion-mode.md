---
id: FEDER-009
title: Add durable ready-queue ingestion mode
status: To Do
assignee: []
created_date: '2026-07-20 20:17'
labels:
  - ingester
  - scheduling
  - file-workflow
  - reliability
dependencies:
  - FEDER-008
references:
  - apps/feder-ingest/src/feder_ingest/__init__.py
  - apps/feder-ingest/src/feder_ingest/db_cache.py
  - apps/feder-ingest/src/feder_ingest/processor.py
documentation:
  - apps/feder-ingest/README.md
priority: high
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Extend finite file ingestion to drain completed scheduled receiver runs safely. Each run directory becomes the ingestion commit unit so abrupt Slurm termination cannot delete the only durable input before database publication.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 feder-ingest --file-input-queue derives the ready directory from receiver.queue-directory and is mutually exclusive with --file-input-directory.
- [ ] #2 Queue mode remains finite and file-only: it constructs neither RabbitMQ nor Prometheus services and takes a single snapshot of ready directories at startup.
- [ ] #3 Ready run directories are validated against the scheduled naming convention and processed oldest interval first; unexpected visible top-level entries cause a non-zero exit.
- [ ] #4 Within each ready run directory, queue mode accepts only visible regular *.nc files; an unexpected visible entry fails that run, while an empty valid run is accepted.
- [ ] #5 All NetCDF files in one run are processed without deleting any input, then DBCache.force_publish succeeds before that run directory and its files are removed.
- [ ] #6 Publication, decoding, validation, or processing failure retains the complete run directory, stops queue processing immediately, and leaves later runs untouched.
- [ ] #7 Any individual trajectory insertion failure is fatal in finite queue mode and retains the input run, while live RabbitMQ ingestion retains its existing log-and-continue behavior.
- [ ] #8 If queue ingestion is interrupted before publication and deletion, the retained run can be retried safely even when staging contains partial or duplicate data.
- [ ] #9 An empty ready queue still calls DBCache.force_publish so dirty durable staging from an earlier failed publication is retried.
- [ ] #10 Automated tests cover fixed-snapshot and oldest-first behavior, strict entry validation, empty runs and queues, insertion and publication failures, process-publish-delete ordering, retained inputs, stopping at the first failure, and successful directory removal.
<!-- AC:END -->
