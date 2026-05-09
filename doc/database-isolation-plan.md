# Database Isolation Plan for GitHub Issue #3

This document captures the design decisions from the planning session for GitHub issue #3, "Database isolation is doubleplus ungood".

## Core invariant

The ingester must never mutate SQLite files in `data-directory`.

The only SQLite databases the ingester may update are:

1. in-memory nursery databases; or
2. durable local staging databases under `staging-directory`.

Files in `data-directory` are public snapshots for clients. They may change only through an atomic publish operation: copy a clean snapshot to a hidden file in the destination directory, then rename/replace it into the final public filename.

## Path roles

Feder must have three separate path roots:

- `data-directory`: shared/public client-visible SQLite snapshots. This may be on NFS. The ingester must not open these files writable and must not create WAL/SHM sidecars here.
- `staging-directory`: durable local ingester storage for mutable on-disk SQLite databases. This must survive ingester restarts.
- `scratch-directory`: ephemeral scratch space. Receiver processes use this for temporary fragment databases. The ingester may use a dedicated subdirectory for export temp snapshots. This may be deleted between restarts.

All three paths must be distinct, and none may be nested inside another. This should be validated both in configuration parsing and in `DBCache`, because `DBCache` may be instantiated directly in tests or future code.

`Config` should validate path semantics but should not create directories. `DBCache` should require `data-directory` to exist and may create `staging-directory` and the dedicated export scratch subdirectory if needed.

## Configuration

Add required path config:

```toml
[paths]
data-directory = "/big/mcast/data/feder"
staging-directory = "/local/durable/feder-staging"
scratch-directory = "/local/scratch/feder-scratch"
```

Add ingester lifecycle tuning:

```toml
[ingester]
prometheus-port = 19001
export-interval = "1 hour"
finalize-after = "12 hours"
```

Defaults:

- `export-interval`: 1 hour
- `finalize-after`: 12 hours

## Directory layout

Staging mirrors the public data layout:

```text
DATA/YYYY/YYYY-DOY.sqlite
STAGING/YYYY/YYYY-DOY.sqlite
```

This allows reuse of the existing date-to-path logic.

## DBCache constructor

`DBCache` should receive explicit values rather than a `Config` object:

```python
DBCache(
    data_dir: str,
    staging_dir: str,
    scratch_dir: str,
    connection_cache_size: int = 16,
    nursery_size: int = 5,
    export_interval: timedelta = timedelta(hours=1),
    finalize_after: timedelta = timedelta(hours=12),
)
```

Call site should pass:

```python
DBCache(
    cfg.data_directory,
    cfg.staging_directory,
    cfg.scratch_directory,
    export_interval=cfg.ingester_export_interval,
    finalize_after=cfg.ingester_finalize_after,
)
```

## Connection lookup order

`DBCache.connect(ref_date)` should use this order:

1. return cached staged connection if present;
2. return nursery connection if present;
3. if a staging file exists, open it writable from `staging-directory`;
4. if a public file exists, import it into staging via hidden temp + rename, then open staging writable;
5. otherwise create a new in-memory nursery DB.

A date should not normally exist in both `_connections` and `_nursery`. If it does, treat that as a bug.

Existing staging is authoritative and takes precedence over public data.

## Public-to-staging import

When a replay/backfill needs a DB that exists only in public data:

1. copy `DATA/YYYY/YYYY-DOY.sqlite` to hidden staging temp `STAGING/YYYY/.YYYY-DOY.sqlite.importing.<pid>`;
2. flush/fsync the temp file;
3. `os.replace(temp, STAGING/YYYY/YYYY-DOY.sqlite)`;
4. best-effort fsync the staging directory;
5. open final staging DB writable/WAL.

Temp import names should not end in `.sqlite`.

If an imported DB is empty, delete only the staging copy and continue as if no DB existed. Do not directly delete public DB files.

## WritableDB behavior

Use `WritableDB(staging_dir, date)` for mutable on-disk DBs. `WritableDB` does not need separate public/staging concepts.

For file-backed writable DBs, enable WAL mode.

For in-memory nursery DBs, avoid setting WAL. Use `journal_mode=OFF` and `synchronous=OFF`.

## Nursery behavior

The in-memory nursery exists for high-rate live/delayed-live ingestion and should remain in memory where possible.

Nursery DBs are promoted to staging only when:

- LRU nursery eviction requires it;
- `end_of_day(day)` is received;
- `close()` is called.

Periodic checkpoint may export nursery snapshots to public data, but must not promote nursery DBs just for checkpointing.

If the ingester restarts, in-memory nursery state since the last public snapshot may be lost. This is accepted to preserve ingestion performance; restarts are rare and receiver replay/backfill mechanisms can handle gaps.

## Nursery promotion

Promotion sequence:

1. `VACUUM INTO` from nursery DB into staging final path.
2. Open/manage the staging DB as the authoritative mutable DB.
3. Export from staging to public via the clean snapshot pipeline.
4. Remove nursery entry only after staging promotion succeeds.

If staging already exists during nursery promotion, treat this as an invariant violation. Do not overwrite staging blindly.

If promotion to staging fails, keep the nursery entry and raise. If promotion succeeds but public export fails, staging is durable and authoritative; mark it dirty and retry export later.

## Clean snapshot export

Never copy a live WAL-mode staging `.sqlite` file directly. Committed data may live in the `-wal` file.

Export a clean snapshot using `VACUUM INTO ?`:

1. Force-commit pending ingester writes before export.
2. `VACUUM INTO ?` to a unique temp snapshot under `scratch-directory/ingester-export`.
3. Open the temp snapshot separately.
4. Ensure `PRAGMA journal_mode=DELETE`.
5. Close the snapshot.
6. Publish it to public data using hidden-copy + rename.
7. Remove scratch temp snapshot.

Use parameterized `VACUUM INTO ?`; Python 3.12+ is guaranteed.

Export temp snapshots may use ordinary names in the dedicated export scratch directory, e.g. `2025-142.export.<pid>.<uuid>.sqlite`.

## Public publish sequence

For final public path `DATA/YYYY/YYYY-DOY.sqlite`:

1. ensure `DATA/YYYY/` exists;
2. copy the scratch snapshot to hidden public temp `DATA/YYYY/.YYYY-DOY.sqlite.exporting.<pid>`;
3. flush/fsync the hidden public temp file;
4. `os.replace(hidden_temp, final_public_path)`;
5. best-effort fsync the destination directory;
6. clean up hidden temp on failure.

The hidden public temp name should not end in `.sqlite`.

Do not rename directly from local scratch/staging to public data as the publish operation; cross-filesystem rename is not atomic and may fail. Copying to a hidden file in the destination directory and then replacing keeps the visible transition atomic within that directory.

File fsync failure is an export failure. Directory fsync failure is best-effort only.

## Export throttling and dirty tracking

Track state by date rather than by DB object:

```python
_touched: set[date]
_last_update: dict[date, datetime]
_last_export: dict[date, datetime]
```

Mark a DB dirty when trajectories are added/deleted. Export clears dirty state for that date.

Checkpoint behavior:

1. `commit(force=True)`;
2. export dirty nursery snapshots if never exported or if `export_interval` elapsed;
3. export dirty staged DBs if never exported or if `export_interval` elapsed;
4. in finalization work, finalize idle staged DBs.

First non-empty DB export should happen at the next checkpoint, not immediately on the first insert.

Export failures during periodic checkpoints are non-fatal. Log, optionally increment the generic ingester error metric, keep the DB dirty, and retry later.

## Startup recovery

On `DBCache` initialization:

1. create required staging/export scratch directories as needed;
2. clean obvious incomplete temp files:
   - staging `.importing.*` files;
   - export scratch temp files;
   - best-effort public `.exporting.*` files if safe;
3. scan `STAGING/????/????-???.sqlite` without eagerly opening all DBs;
4. for each staged DB, derive:
   - `last_update` from max mtime of `.sqlite`, `-wal`, and `-shm`;
   - dirty if public missing or staging mtime is newer than public mtime;
   - `last_export` from public mtime if present.

Dirty recovered staged DBs should be exported/finalized by later checkpoint processing even if no new trajectories touch that date.

For export-only unopened staged files, prefer a raw read-only SQLite connection using `mode=ro` and no `immutable=1`. Fall back to read-write if needed, but do not force WAL just for export.

## Finalization

A staged DB is eligible for finalization after `finalize_after` idle time, default 12 hours.

Finalization sequence:

1. force commit if open;
2. force export clean snapshot;
3. close any open staging connection;
4. delete staging `.sqlite`, `.sqlite-wal`, and `.sqlite-shm`;
5. remove associated metadata.

If finalization export fails, do not delete staging files.

`end_of_day(day)` should force promotion/export but should not immediately delete staging. Late updates or replay may still arrive.

`close()` should commit and promote non-empty nursery DBs to staging, attempt export, and leave non-finalized staging files in place. It should not delete active staged DBs just because the process is stopping or restarting.

## Error handling

- Periodic export failure: non-fatal; keep dirty and retry.
- Public export failure after successful staging promotion: non-fatal; staging is authoritative.
- Promotion-to-staging failure: fatal for that operation; do not drop nursery entry.
- Path validation failures: fail fast.

## Logging and metrics

Log key lifecycle events at `INFO`:

- imported public database to staging;
- promoted nursery database to staging;
- exported database snapshot, including duration;
- finalized staged database.

Log failures at `ERROR`/`EXCEPTION`. Use the existing generic ingester error metric for export failures if practical. Dedicated Prometheus metrics can be added later if needed.

## Test coverage

The complete #3 work should cover:

1. Config requires `paths.staging-directory`.
2. `DBCache.connect()` creates nursery only when no staging/public exists.
3. Nursery checkpoint exports clean public snapshot without creating staging.
4. Nursery promotion writes to staging, then publishes public snapshot.
5. Existing public DB import copies to staging and writes only staging.
6. Existing staging takes precedence over public.
7. Exported public DB has no WAL mode/sidecars and can open `mode=ro&immutable=1`.
8. Public publish uses hidden temp then rename, and hidden temp names do not match `*.sqlite`.
9. Idle finalization exports, closes, deletes staging DB/WAL/SHM.
10. `close()` does not delete non-idle staging DBs.

Additional path safety tests:

- the three path roots must be distinct;
- none may be nested in another;
- validation exists in both `Config` and direct `DBCache` construction.

## GitHub implementation issues

The plan was persisted as these GitHub issues:

- #4 Add ingester staging-directory config and path safety validation
- #5 Implement clean SQLite snapshot export and atomic public publish
- #6 Rework DBCache around durable staging and public import isolation
- #7 Add staging recovery, throttled export, and idle finalization
- #8 Complete tests and operator docs for database isolation
