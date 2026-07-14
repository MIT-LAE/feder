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
  contrails-api
```

File-output mode is deliberately finite and historical-only in v1:

- both `--start-time` and `--end-time` are required;
- file/glob-only sources are rejected;
- live receiver mode still uses RabbitMQ;
- RabbitMQ and ingester Prometheus config are not required and no RabbitMQ client is constructed.

The output directory is created if needed, must be distinct from and not nested under any configured `[paths]` root, and must contain no visible `*.nc` files at startup. The recommended operational pattern is to create a unique output directory for each receiver run and pass that directory to one ingester run after the receiver exits successfully.

Each completed batch is written to a hidden temporary file, fsynced best-effort, and published with an atomic `os.replace` to a visible name like:

```text
rx-contrails-api-12345.00000001.nc
```

No manifest is written. The handoff contract is simply: visible, non-hidden `*.nc` files in the run directory are complete NetCDF trajectory-batch files. Hidden temporary files are implementation details and should not be consumed.

If NetCDF writing or atomic publication fails, the receiver deletes its temporary file, logs the failure, and exits non-zero. Source trajectories are removed from receiver staging only after the corresponding NetCDF file has been successfully published.

## NetCDF interchange format

Feder NetCDF files store the RabbitMQ-equivalent `TrajectoryBatch` payload as a CF-1.8 discrete sampling geometry contiguous ragged trajectory array:

- global attributes include `Conventions = "CF-1.8"`, `featureType = "trajectory"`, `feder_file_type = "feder.trajectory_batch"`, `feder_file_version = "1.0"`, `feder_source`, `feder_trajectory_count`, and `feder_created_utc`;
- the `trajectory` dimension indexes aircraft trajectories and the `obs` dimension stores all observations contiguously;
- `trajectory_id` has `cf_role = "trajectory_id"` and `row_size(sample_dimension="obs")` gives the number of observations for each trajectory;
- per-trajectory variables include source, transponder id, callsign, origin, destination, and aircraft type;
- per-observation variables include time, longitude, latitude, altitude, GNSS altitude, heading, and on-ground state.

Empty end-of-day marker batches are not representable in this file protocol.
