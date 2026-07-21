# Slurm scheduled file-processing operation

These templates operate the durable file-only Contrails workflow. They do not
start RabbitMQ or Prometheus. Copy the four `*.template` files in this directory
to a site-controlled location, replace every `REPLACE_WITH_*` value, and retain
site-specific module/environment activation in the sbatch scripts.

- `feder-rx-scheduled.sbatch.template` runs one cursor-managed receiver chunk.
- `submit-feder-rx-scheduled.template` submits it every six hours.
- `feder-ingest-ready-queue.sbatch.template` drains one ready-queue snapshot.
- `submit-feder-ingest-ready-queue.template` submits it daily.

The submission wrappers first perform a best-effort `squeue` check, then submit
with `--dependency=singleton` and stable, distinct job names. The singleton
dependency is the concurrency control; do **not** add shared-filesystem file
locks. Receiver and ingester jobs can overlap safely because they have separate
singleton names and communicate only through atomic ready-directory publication.
Install the shown cron examples in the submission templates (six-hourly for the
receiver, daily for the ingester), or configure equivalent Slurm-aware external
scheduling.

## Required configuration and storage

Use `config.toml.template` as the source for these values:

```toml
[receiver]
queue-directory = "/shared/durable/feder-rx-queue"
max-run-duration = "24 hours"

[source.contrails-api]
data-lag = "48 hours"
```

`queue-directory` must be a durable shared filesystem location, distinct from
and not nested under every `[paths]` root. Its filesystem must support durable
atomic directory rename: the receiver publishes a complete run by renaming it
from `incomplete/` to `ready/`. It is not a locking protocol. The normal
maximum receiver chunk is 24 hours; the availability cutoff is the current time
minus the configured Contrails `data-lag` (48 hours in the template).

The durable queue layout is:

```text
QUEUE/
  cursor.json
  incomplete/
    contrails-api-YYYYMMDDTHHMMSSZ-YYYYMMDDTHHMMSSZ-<uuid>/
  ready/
    contrails-api-YYYYMMDDTHHMMSSZ-YYYYMMDDTHHMMSSZ-<uuid>/
      rx-contrails-api-*.nc
```

Run names use UTC, half-open start/end boundaries and a UUID. `cursor.json`
contains the next interval start. `incomplete/` holds uncommitted work; `ready/`
holds complete run directories. Empty ready runs are valid commits.

## Bootstrap, normal operation, and catch-up

Bootstrap the cursor exactly once by setting `INITIAL_START_TIME` in the
receiver sbatch template to a whole-hour UTC value, for example
`2026-07-01T00:00:00+00:00`. The first run durably writes that cursor before any
download. Clear the setting after the first successful submission: later runs
must read the durable cursor and do not require it.

A scheduled receiver invocation handles at most one chunk, bounded by
`max-run-duration` and the lagged availability cutoff. Thus, after a maintenance
window it catches up one chunk per six-hourly job rather than submitting a large
backfill. Exiting with no work because the cursor is at or ahead of the cutoff
is normal and successful. The daily ingester likewise exits successfully when
`ready/` is empty (while still publishing recovered dirty state if necessary).

The ingester takes one startup snapshot of `ready/`, processes runs oldest
first, and removes a run only after all files are ingested and public snapshots
are published. Ready runs are therefore retained across downtime and failures.
A receiver crash after publishing a ready run but before advancing the cursor
may repeat an interval; a failed ingester retains the complete run. Rerun the
corresponding finite job to recover—this is deliberately duplicate-safe and
prevents gaps.

Inspect stale directories before deleting anything:

```bash
find /shared/durable/feder-rx-queue/incomplete -mindepth 1 -maxdepth 1 -type d -print
find /shared/durable/feder-rx-queue/ready -mindepth 1 -maxdepth 1 -type d -print
```

Do not remove `ready/` runs merely because they are old: they are retryable
commit units. An `incomplete/` directory is never reused by Feder; after
confirming no receiver job owns it and preserving any needed evidence, an
operator may remove that stale incomplete directory manually. Investigate a
stuck ready run from its job log and rerun the ingester; remove it only after
confirming its contents were safely ingested/published or are intentionally
discarded.

## Manual receiver recovery

For a known missing historical interval, run the regular finite receiver with
an explicit half-open UTC range, not the cursor-managed command:

```bash
feder-rx --config /path/to/config.toml \
  --start-time 2026-07-01T00:00:00+00:00 \
  --end-time 2026-07-02T00:00:00+00:00 \
  --file-output-directory /shared/recovery/contrails-20260701 \
  contrails-api
feder-ingest --config /path/to/config.toml \
  --file-input-directory /shared/recovery/contrails-20260701
```

Both times must be whole UTC hours; the end is exclusive (`[start, end)`). Use
a fresh recovery directory and run the ingester only after the receiver exits
successfully. This manual workflow does **not** read or modify the scheduled
`cursor.json`; reconcile intentional overlap or gaps operationally before
returning to the scheduled queue.
