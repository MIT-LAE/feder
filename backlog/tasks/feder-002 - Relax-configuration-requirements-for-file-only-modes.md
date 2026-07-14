---
id: FEDER-002
title: Relax configuration requirements for file-only modes
status: In Progress
assignee:
  - '@pi'
created_date: '2026-07-13 21:31'
updated_date: '2026-07-14 08:51'
labels:
  - file-mode
  - config
  - cluster
dependencies: []
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Allow Feder receiver and ingester file-only modes to run on cluster systems without RabbitMQ, Prometheus, or Mailjet configuration. Normal RabbitMQ/live modes should keep their existing strict requirements where applicable.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Config loading supports mode-specific requirements so file-only rx/ingest do not require rabbitmq keys
- [ ] #2 feder-ingest --file-input-directory does not require Prometheus configuration and does not start a Prometheus server by default
- [ ] #3 Mailjet configuration is not required by feder-rx or feder-ingest runs and remains required only where actually used
- [ ] #4 File-only modes accept the shared Feder path configuration needed by the ingester and validate configured roots as before
- [ ] #5 Normal RabbitMQ receiver and ingester modes retain existing RabbitMQ behavior and fail clearly when required RabbitMQ configuration is missing
- [ ] #6 Config tests or CLI tests cover omitted rabbitmq, monitoring/prometheus, and mailjet sections in file-only mode
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Audit Config consumers in feder-rx, feder-ingest, state-of-feder, and server modules to identify which config sections each command actually needs.
2. Refactor Config initialization to support feature flags or optional section groups for RabbitMQ, monitoring/Prometheus, Mailjet, paths, ingester, and sources without weakening normal modes.
3. Update feder-rx and feder-ingest CLI startup to choose stricter RabbitMQ requirements in existing modes and relaxed requirements in file-only modes.
4. Make Mailjet optional for rx/ingest paths and preserve or move strict Mailjet validation to state-of-feder or the component that sends mail.
5. Disable or make optional ingester Prometheus startup in --file-input-directory mode.
6. Add tests for file-only configs omitting rabbitmq, prometheus/monitoring, and mailjet sections, plus regression tests that normal RabbitMQ modes still fail clearly when required config is missing.
7. Update config template comments if needed to describe optional sections.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
- Added ConfigRequirements and named requirement profiles for strict/default, file-only, receiver, ingester, and state-of-feder modes.
- Updated feder-rx/feder-ingest to stop requiring Mailjet; feder-rx also no longer requires ingester Prometheus config.
- Updated state-of-feder to require Mailjet only, not RabbitMQ/ingester Prometheus.
- Added config tests covering omitted rabbitmq, ingester prometheus-port, and mailjet sections for mode-specific requirements, plus live RabbitMQ missing-config regression.
- Updated config.toml.template comments to document optional sections for file-only workflows.
<!-- SECTION:NOTES:END -->
