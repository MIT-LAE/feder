---
id: FEDER-010
title: Document and template Slurm operation for scheduled file processing
status: In Progress
assignee:
  - '@myself'
created_date: '2026-07-20 20:17'
updated_date: '2026-07-20 20:18'
labels:
  - deployment
  - slurm
  - documentation
  - file-workflow
dependencies:
  - FEDER-008
  - FEDER-009
references:
  - config.toml.template
  - deploy/feder-rx-contrails-api.service
  - deploy/feder-ingest.service
documentation:
  - apps/feder-rx/README.md
  - apps/feder-ingest/README.md
priority: medium
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Provide operator-ready examples for running the scheduled Contrails receiver every six hours and the ready-queue ingester daily on a Slurm cluster, including maintenance-window catch-up and failure recovery.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The configuration template documents receiver.queue-directory, the 24-hour default maximum run duration, source data lag, and the durable shared-filesystem and atomic-rename requirements.
- [ ] #2 A receiver submission template performs an squeue pre-check and submits a six-hourly job with a stable receiver job name and Slurm singleton dependency.
- [ ] #3 An ingester submission template performs an squeue pre-check and submits a daily job with a distinct stable ingester job name and Slurm singleton dependency.
- [ ] #4 Receiver and ingester sbatch templates include configurable placeholders for account, partition, time limit, paths, logs, modules/environment, and Slurm failure notifications.
- [ ] #5 Templates do not rely on shared-filesystem file locking and permit receiver and ingester jobs to run concurrently under their separate singleton names.
- [ ] #6 Operator documentation explains one-time cursor bootstrap, normal no-work exits, one-chunk-per-job catch-up after downtime, retained ready runs, duplicate-safe crash recovery, and stale incomplete-directory inspection and cleanup.
- [ ] #7 Operator documentation includes manual explicit-range receiver recovery using half-open UTC ranges and explains that manual runs do not modify the scheduled cursor.
- [ ] #8 The documented directory layout and run naming convention match the implemented cursor, incomplete, and ready queue behavior.
- [ ] #9 Shell templates receive syntax checks where available without requiring a Slurm installation in automated tests.
<!-- AC:END -->
