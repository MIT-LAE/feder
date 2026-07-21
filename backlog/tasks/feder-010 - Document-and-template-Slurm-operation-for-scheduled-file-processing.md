---
id: FEDER-010
title: Document and template Slurm operation for scheduled file processing
status: Done
assignee:
  - '@myself'
created_date: '2026-07-20 20:17'
updated_date: '2026-07-21 07:17'
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
- [x] #1 The configuration template documents receiver.queue-directory, the 24-hour default maximum run duration, source data lag, and the durable shared-filesystem and atomic-rename requirements.
- [x] #2 A receiver submission template performs an squeue pre-check and submits a six-hourly job with a stable receiver job name and Slurm singleton dependency.
- [x] #3 An ingester submission template performs an squeue pre-check and submits a daily job with a distinct stable ingester job name and Slurm singleton dependency.
- [x] #4 Receiver and ingester sbatch templates include configurable placeholders for account, partition, time limit, paths, logs, modules/environment, and Slurm failure notifications.
- [x] #5 Templates do not rely on shared-filesystem file locking and permit receiver and ingester jobs to run concurrently under their separate singleton names.
- [x] #6 Operator documentation explains one-time cursor bootstrap, normal no-work exits, one-chunk-per-job catch-up after downtime, retained ready runs, duplicate-safe crash recovery, and stale incomplete-directory inspection and cleanup.
- [x] #7 Operator documentation includes manual explicit-range receiver recovery using half-open UTC ranges and explains that manual runs do not modify the scheduled cursor.
- [x] #8 The documented directory layout and run naming convention match the implemented cursor, incomplete, and ready queue behavior.
- [x] #9 Shell templates receive syntax checks where available without requiring a Slurm installation in automated tests.
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Review existing deploy assets, command installation paths, and file-only documentation so the cluster templates follow current project conventions without replacing site-specific configuration.
2. Update config.toml.template and receiver/ingester documentation with queue configuration, directory layout, half-open run names, shared-filesystem atomic-rename assumptions, and one-time cursor bootstrap.
3. Add receiver and ingester sbatch templates with distinct stable job names, singleton dependencies, finite commands, log paths, time limits, failure notification placeholders, and configurable account/partition/environment setup.
4. Add cron-facing submission wrappers that use squeue as a best-effort pre-check before sbatch, while documenting singleton as the actual concurrency guarantee and avoiding shared-filesystem locks.
5. Document six-hour receiver and daily ingester schedules, one-24-hour-chunk catch-up behavior, fixed ingestion snapshots, no-work exits, retained ready runs, duplicate-safe crash recovery, manual half-open recovery, and stale incomplete-directory cleanup.
6. Add lightweight shell syntax checks where supported, verify all examples against the implemented CLI help/config names, and run documentation/config-related tests.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
- Added Slurm sbatch and cron-facing submission templates with independent singleton job names.
- Documented queue lifecycle, recovery, and manual range backfill; added shell syntax/template coverage.
- Ran full pytest with a local RabbitMQ container: 130 passed, 1 skipped.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Implemented FEDER-010 Slurm operation support for scheduled file processing.

Changes:
- Added configurable receiver and ingester sbatch templates plus six-hourly/daily squeue + singleton submission wrappers.
- Documented durable queue layout, cursor bootstrap, catch-up, retry/recovery, stale-directory handling, and manual half-open UTC range recovery.
- Expanded configuration guidance and linked receiver/ingester operator docs.
- Added Bash syntax and template-contract tests.

Tests:
- uv run pytest (130 passed, 1 skipped; RabbitMQ test container configured locally)
<!-- SECTION:FINAL_SUMMARY:END -->
