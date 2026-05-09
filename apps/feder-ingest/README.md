# Feder ingester

The ingester consumes trajectory batches from RabbitMQ and maintains daily SQLite databases. It uses three separate storage roots configured in `[paths]`:

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
