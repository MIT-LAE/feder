---
id: FEDER-002
title: Relax configuration requirements for file-only modes
status: To Do
assignee: []
created_date: '2026-07-13 21:31'
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
