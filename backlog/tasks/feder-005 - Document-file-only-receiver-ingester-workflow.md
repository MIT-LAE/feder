---
id: FEDER-005
title: Document file-only receiver-ingester workflow
status: To Do
assignee: []
created_date: '2026-07-13 21:32'
labels:
  - file-mode
  - docs
  - operations
dependencies: []
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Document the new NetCDF file-only workflow for running Feder receiver and ingester jobs on clusters without RabbitMQ or Prometheus, including recommended Slurm/cron handoff patterns and operational failure semantics.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Receiver and ingester READMEs describe --file-output-directory and --file-input-directory usage
- [ ] #2 Operator docs explain the recommended finite workflow: receiver writes a unique run directory, then ingester consumes it
- [ ] #3 Docs state that same-directory concurrent operation is atomic-safe but not the recommended v1 workflow
- [ ] #4 Docs describe directory validation, empty output requirements, input file ordering, ignored files, deletion semantics, and invalid NetCDF failure behavior
- [ ] #5 Config template or documentation explains which RabbitMQ, Prometheus, and Mailjet settings are optional in file-only cluster mode
- [ ] #6 Docs mention that no manifest is used and visible atomically published *.nc files are the handoff contract
- [ ] #7 Docs include a concise description of the NetCDF CF contiguous ragged-array format and version attributes
<!-- AC:END -->
