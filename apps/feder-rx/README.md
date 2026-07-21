# Feder receiver

The receiver collects trajectory data from one source and normally publishes completed trajectory batches to RabbitMQ for the ingester. It can also run in finite NetCDF file-output mode for clusters where RabbitMQ and Prometheus are not available.

## NetCDF file-output mode

Use `--file-output-directory` to write completed historical trajectory batches as atomically published NetCDF files instead of sending them to RabbitMQ:

```bash
run_id="$(date -u +%Y%m%dT%H%M%SZ)-contrails"
out="/shared/feder/rx-runs/${run_id}"

feder-rx \
  --config /path/to/file-only-config.toml \
  --start-time 2026-07-01T00:00:00+00:00 \
  --end-time 2026-07-02T00:00:00+00:00 \
  --file-output-directory "${out}" \
  --file-output-max-trajectories 10000 \
  contrails-api
```

File-output mode is deliberately finite and historical-only in v1:

- both `--start-time` and `--end-time` are required;
- Contrails historical ranges are UTC hourly half-open intervals `[start-time, end-time)`; both boundaries must fall exactly on a whole UTC hour, and the end-hour file is not requested;
- every hourly file in the requested interval must be retrieved and processed for the run to succeed. Authentication and invalid requests fail immediately; missing, rate-limited, server, timeout, and connection failures retry up to five times (normally about five minutes apart, honoring a bounded `Retry-After`) and then cause a non-zero exit;
- file/glob-only sources are rejected;
- live receiver mode still uses RabbitMQ;
- RabbitMQ and ingester Prometheus config are not required and no RabbitMQ client is constructed.

The output directory is created if needed, must be distinct from and not nested under any configured `[paths]` root, and must contain no visible `*.nc` files at startup. The recommended operational pattern is to create a unique output directory for each receiver run and pass that directory to one ingester run after the receiver exits successfully.

Completed trajectory batches are aggregated before being written to NetCDF. By default, `feder-rx` buffers up to 10,000 completed trajectories per output file; override this with `--file-output-max-trajectories N` if you need smaller or larger files. The threshold is deliberately simple: when the buffer reaches or exceeds the configured trajectory count, the receiver publishes one aggregate file. The final partial buffer is flushed before a successful finite receiver run exits.

Each aggregate file is written to a hidden temporary file, fsynced best-effort, and published with an atomic `os.replace` to a visible name like:

```text
rx-contrails-api-12345.00000001.nc
```

No manifest is written. The handoff contract is simply: visible, non-hidden `*.nc` files in the run directory are complete NetCDF trajectory-batch files. Hidden temporary files are implementation details and should not be consumed.

If NetCDF writing or atomic publication fails, the receiver deletes its temporary file, logs the failure, and exits non-zero. In aggregate file-output mode, completed trajectories are removed from receiver staging after they have been materialized into the sink's in-memory buffer so they cannot be rediscovered by later completion cycles. Because buffered trajectories may not yet have been durably published when a failure occurs, operators should only hand an output directory to `feder-ingest` after `feder-rx` exits successfully. If a receiver run fails, rerun the historical receiver job into a fresh output directory.

## Scheduled Contrails runs

`feder-rx-scheduled` performs one finite, cursor-managed Contrails interval per invocation. Configure an isolated queue root (it must not overlap any `[paths]` root):

```toml
[receiver]
queue-directory = "/shared/feder/receiver-queue"
max-run-duration = "24 hours" # positive whole number of hours
```

Bootstrap the queue once with an explicit whole-hour UTC start; later runs read the durable cursor and do not take this option:

```bash
feder-rx-scheduled --config /path/to/config.toml \
  --initial-start-time 2026-07-01T00:00:00+00:00 contrails-api
```

The command floors `now - source.data-lag` to an hour and processes at most `max-run-duration`. It writes each run under `incomplete/`, then atomically moves the complete directory (including an intentionally empty successful run) to `ready/`. Only after that move does it atomically advance `cursor.json`; a crash may therefore repeat a run, but cannot create a gap. Handled failures remove their own incomplete directory; pre-existing incomplete directories are retained for operator inspection and are never reused.

For production Slurm schedules, use the receiver template and operational procedure in [`deploy/README-slurm.md`](../../deploy/README-slurm.md). It configures a six-hourly finite invocation without RabbitMQ or Prometheus.

## NetCDF interchange format

Feder NetCDF files store the RabbitMQ-equivalent `TrajectoryBatch` payload as a CF-1.8 discrete sampling geometry contiguous ragged trajectory array:

- global attributes include `Conventions = "CF-1.8"`, `featureType = "trajectory"`, `feder_file_type = "feder.trajectory_batch"`, `feder_file_version = "1.0"`, `feder_source`, `feder_trajectory_count`, and `feder_created_utc`;
- the `trajectory` dimension indexes aircraft trajectories and the `obs` dimension stores all observations contiguously;
- `trajectory_id` has `cf_role = "trajectory_id"` and `row_size(sample_dimension="obs")` gives the number of observations for each trajectory;
- per-trajectory variables include source, transponder id, callsign, origin, destination, and aircraft type;
- per-observation variables include time, longitude, latitude, altitude, GNSS altitude, heading, and on-ground state.

Empty end-of-day marker batches are not representable in this file protocol.
