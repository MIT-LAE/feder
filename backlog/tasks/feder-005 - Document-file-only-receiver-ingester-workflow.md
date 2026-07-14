---
id: FEDER-005
title: Document file-only receiver-ingester workflow
status: In Progress
assignee:
  - '@pi'
created_date: '2026-07-13 21:32'
updated_date: '2026-07-14 13:47'
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

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Review current receiver, ingester, and configuration documentation to find the right locations for file-only workflow instructions.
2. Document receiver --file-output-directory usage, historical-only requirements, output directory rules, file naming, and failure semantics.
3. Document ingester --file-input-directory usage, finite batch behavior, lexicographic processing, ignored files, invalid file behavior, deletion semantics, empty-input retry, and forced final publish.
4. Add an operator workflow section for cron/Slurm: receiver writes a unique run directory, exits, ingester consumes that directory, and wrappers handle archiving/removal.
5. Note that concurrent same-directory operation is atomic-safe but not the recommended v1 operating model.
6. Document optional cluster-mode configuration expectations for RabbitMQ, Prometheus, and Mailjet.
7. Include a concise NetCDF protocol summary: CF contiguous ragged array, one file per batch, no manifest, lightweight Feder version attributes.
8. Review docs for consistency with implemented CLI names and tests before marking complete.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
- Added receiver README documentation for finite NetCDF file-output mode, atomic publication, handoff contract, and CF ragged-array format.
- Added ingester README documentation for finite file-input mode, validation, lexicographic processing, deletion/failure semantics, operator handoff workflow, concurrent same-directory caveat, and NetCDF format.
- Clarified config template source Prometheus ports are optional for historical receiver file-output jobs.
<!-- SECTION:NOTES:END -->
