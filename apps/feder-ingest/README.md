# Feder ingester

The ingester consumes trajectory batches from RabbitMQ, or from a finite directory of NetCDF trajectory-batch files in file-input mode, and maintains daily SQLite databases. It uses three separate storage roots configured in `[paths]`:

- `data-directory`: public, client-visible SQLite snapshots. This can be shared storage such as NFS. The ingester does not open these databases writable and should not create SQLite WAL/SHM sidecars here.
- `staging-directory`: durable local ingester storage for mutable SQLite databases. This must survive ingester restarts. Staging mirrors the public layout: `YYYY/YYYY-DOY.sqlite`.
- `scratch-directory`: ephemeral scratch space. The ingester uses `scratch-directory/ingester-export` for temporary export snapshots. This may be cleared between restarts.

The three roots must be distinct, and none may be nested inside another.

## Public snapshot publishing

Public files in `data-directory` are snapshots for readers. They are changed only by the publish pipeline:

1. create a clean SQLite snapshot with `VACUUM INTO` under `scratch-directory/ingester-export`;
2. copy that snapshot to a hidden temporary file in the destination public directory;
3. flush/fsync the hidden file;
4. atomically replace the visible public database with `os.replace`.

This keeps client-visible transitions atomic within the public directory and avoids copying live WAL-mode staging database files directly.

## Staging lifecycle

When the ingester needs to mutate a date that already exists publicly, it first imports the public snapshot into `staging-directory` using a hidden temp file and atomic rename. Existing staging files are authoritative and take precedence over public snapshots.

New dates begin in an in-memory nursery for ingestion performance. Nursery databases are exported to public snapshots during checkpointing without being promoted. They are promoted to durable staging only on nursery eviction, end-of-day handling, or shutdown.

On startup, the ingester scans existing staging files, reconstructs dirty/export metadata, and retries exports as needed. Idle staged databases are finalized after `ingester.finalize-after`: the ingester exports a final public snapshot and then deletes the staging database and sidecars. Shutdown does not delete active staged databases.

## NetCDF file-input mode

Use `--file-input-directory` to process visible NetCDF trajectory-batch files from a directory and then exit:

```bash
feder-ingest \
  --config /path/to/file-only-config.toml \
  --file-input-directory /shared/feder/rx-runs/20260701T000000Z-contrails
```

File-input mode is deliberately finite and file-only in v1. It does not start RabbitMQ, does not start the Prometheus HTTP server, and may use a configuration file that omits the `[rabbitmq]`, `[mailjet]`, and `ingester.prometheus-port` settings. The ingester still needs `[paths]` and the ingester `export-interval` / `finalize-after` settings if you want non-default values.

The input directory must already exist, must be a directory, and must be distinct from and not nested under `paths.data-directory`, `paths.staging-directory`, or `paths.scratch-directory`. This prevents a handoff directory from overlapping the public SQLite snapshots or mutable ingester storage.

Input entries are scanned once and sorted lexicographically by file name. For each entry:

- hidden entries such as `.partial.nc` or temporary files are skipped;
- visible regular files ending in `.nc` are read as Feder NetCDF trajectory-batch files;
- other visible entries are ignored with a warning;
- after a valid file is processed successfully, it is deleted from the input directory;
- after all valid visible NetCDF files have been processed, the ingester forces a final public snapshot publish before exiting.

If an input file is not valid NetCDF, has the wrong Feder file type/version, is missing required CF/Feder fields, or otherwise cannot be read, the ingester fails and exits non-zero. The failing file is retained, and later lexicographic files are not processed. If processing a valid batch fails, the input file is also retained because deletion happens only after successful ingestion.

No manifest is used. Visible, non-hidden `*.nc` files are the handoff contract between the receiver and ingester.

## Scheduled ready-queue mode

Use `--file-input-queue` to drain completed runs published by
`feder-rx-scheduled` under `receiver.queue-directory/ready`:

```bash
feder-ingest --config /path/to/config.toml --file-input-queue
```

This is also finite and file-only: it starts neither RabbitMQ nor Prometheus.
The receiver owns the queue root and creates the `ready` directory; the
configuration must include `receiver.queue-directory`, which must not overlap
any storage root. At startup the ingester takes one fixed snapshot of visible
ready directories, validates scheduled Contrails run names, and processes the
intervals oldest first. Hidden temporary entries are ignored; any other visible
top-level entry is an error.

A ready run is the commit unit. It may be empty, but otherwise can contain only
visible regular `*.nc` files. The ingester processes every file, force-publishes
all dirty database state, and only then deletes the files and run directory. A
decode, validation, insertion, or publish error retains that complete run and
stops before later runs, so rerunning the command safely retries it. An empty
ready queue still force-publishes recovered dirty staging state.

## Recommended file-only operator workflow

The recommended v1 cluster workflow is a finite two-job handoff:

1. A receiver job writes to a unique run directory, for example `/shared/feder/rx-runs/$SLURM_JOB_ID-$SOURCE-$START-$END`, using `feder-rx --file-output-directory`.
2. The receiver exits successfully only after all complete batches have been atomically published as visible `*.nc` files.
3. A dependent ingester job consumes exactly that run directory using `feder-ingest --file-input-directory`.
4. The ingester deletes each file after successful processing, forces a final publish to `paths.data-directory`, and exits.
5. A wrapper, cron job, or Slurm epilogue can then archive or remove the run directory if it is empty.

A typical Slurm handoff is:

```bash
rx_job=$(sbatch --parsable rx-file-output.sbatch)
sbatch --dependency=afterok:${rx_job} ingest-file-input.sbatch
```

A cron or shell-wrapper handoff can use the same rule: create a fresh run directory, run the receiver, and only if it exits with status 0 run the ingester on that directory.

Same-directory concurrent operation is atomic-safe because receivers publish files with an atomic rename and the ingester only consumes visible non-hidden `*.nc` files. However, it is not the recommended v1 workflow: running a finite receiver to completion and then running one finite ingester gives clearer failure boundaries, simpler retries, and easier directory lifecycle management.

## NetCDF interchange format

Feder NetCDF files store the RabbitMQ-equivalent `TrajectoryBatch` payload as a CF-1.8 discrete sampling geometry contiguous ragged trajectory array:

- global attributes include `Conventions = "CF-1.8"`, `featureType = "trajectory"`, `feder_file_type = "feder.trajectory_batch"`, `feder_file_version = "1.0"`, `feder_source`, `feder_trajectory_count`, and `feder_created_utc`;
- the `trajectory` dimension indexes trajectories and the `obs` dimension stores all point observations contiguously;
- `row_size(sample_dimension="obs")` gives the number of observations for each trajectory;
- `trajectory_id` is the CF trajectory id variable;
- per-trajectory variables carry source and aircraft metadata, and per-observation variables carry time, lon/lat, altitude, GNSS altitude, heading, and on-ground state.

Empty RabbitMQ end-of-day marker batches are not representable in this file protocol.
