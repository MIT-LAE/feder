# bz2 → lz4 migration plan (Feder 1.0.0)

## Motivation

After the numpy vectorisation and thread-pool changes, profiling shows that
bz2 decompression of ~1 950 trajectory blobs per query now accounts for the
bulk of remaining latency (~0.51 s serial, reduced to ~0.18 s median with 8
threads).  lz4 decompresses at ~6 GB/s vs bz2's ~10 MB/s — roughly 600×
faster — which would reduce decompression to near-zero and push all query
latency percentiles comfortably under 100 ms.

This change requires converting 380 GB of historical data (1 307 daily
SQLite files) and breaking backwards compatibility with existing data files.
It will be released together with the numpy and thread-pool speedups as
Feder **1.0.0**, using the ~10× end-to-end query speedup as leverage for the
format break.

---

## Blob format specification

### Current format
```
points column = bz2.compress(Point.pack(traj.points))
```
No version tag; format is implicit in the library version.

### New format
```
points column = b'\x01' + lz4.frame.compress(Point.pack(traj.points))
```

- **Byte 0** — format version tag.  `0x01` = lz4.frame.  Future migrations
  add new version bytes without requiring another flag day.
- **Bytes 1…** — standard lz4 frame format (self-describing, includes
  original size; no separate size field needed on decompression).

`feder_server/messages.py` uses bz2 for RPC transport compression.  That is
a separate concern and is **not changed** by this migration.

---

## Code changes

### `libs/common` — read path (`feder_common/db.py`)

Replace the decompression call in `_process_blob`:

```python
# Before
arr = Point._unpack_blob(bz2.decompress(blob))

# After
import lz4.frame
version = blob[0]
if version != 0x01:
    raise ValueError(
        f'unsupported blob version {version:#04x} — '
        'is the feder library up to date?'
    )
arr = Point._unpack_blob(lz4.frame.decompress(blob[1:]))
```

Remove `import bz2` from `db.py` (no longer used for storage).

Add `lz4` to `libs/common/pyproject.toml`:
```toml
dependencies = ["numpy>=1.24", "lz4>=4.0"]
```

### `apps/feder-ingest` — write path (`feder_ingest/writeable_db.py`)

Replace the compression call in `add_trajectory`:

```python
# Before
bz2.compress(Point.pack(traj.points))

# After
import lz4.frame
b'\x01' + lz4.frame.compress(Point.pack(traj.points))
```

Remove `import bz2`.

Add `lz4` to `apps/feder-ingest/pyproject.toml`:
```toml
dependencies = [..., "lz4>=4.0"]
```

### Version bump

Bump all packages to `1.0.0` (using `bump-my-version`).

---

## Scripts (`scripts/`)

Four standalone scripts, all run manually on the cluster.  None are wired
into CI (the data volumes are too large).

### 1. `convert_file.py` — per-file conversion (called by Slurm)

```
python scripts/convert_file.py <src.sqlite> <dst.sqlite>
```

Algorithm:
1. Open source file read-only.
2. Use the SQLite backup API (`src_conn.backup(dst_conn)`) to copy the
   entire database to the destination, preserving the R-tree index and all
   metadata unchanged.
3. In a single transaction, `UPDATE trajectories SET points=?` for every
   row: `b'\x01' + lz4.frame.compress(bz2.decompress(old_blob))`.
4. `PRAGMA integrity_check` on the destination — fail loudly if it reports
   anything other than `ok`.
5. Assert `SELECT COUNT(*) FROM trajectories` matches between source and
   destination — fail loudly on mismatch.
6. Exit 0 on success, non-zero on any failure (so Slurm marks the job
   failed).

### 2. `bulk_convert.sh` — Slurm job array

```bash
sbatch scripts/bulk_convert.sh /data2/feder/main /data2/feder/temp_lz4
```

- `--array=0-1306%20` — one task per file, 20 running concurrently.
- Each task resolves its file by index, calls `convert_file.py`, and writes
  stdout/stderr to per-task log files for post-hoc inspection.
- After all tasks complete, run `scripts/verify_counts.py` (see below) as a
  final sweep.

### 3. `tail_convert.sh` — daily cron for new files

Once bulk conversion is complete, schedule as a daily cron job:

```
0 6 * * * python /path/to/scripts/convert_file.py \
    /data2/feder/main/YYYY/YYYY-DDD.sqlite \
    /data2/feder/temp_lz4/YYYY/YYYY-DDD.sqlite
```

Converts yesterday's closed day-file each morning.  Runs until cutover.

### 4. `verify_counts.py` — row-count sweep

```
python scripts/verify_counts.py <bz2_dir> <lz4_dir>
```

For every file pair, assert `SELECT COUNT(*) FROM trajectories` matches.
Reports any mismatches and exits non-zero if any are found.  Fast — runs
in minutes across all 1 307 files.

### 5. `verify_sample.py` — decompress-and-compare spot check

```
python scripts/verify_sample.py <bz2_dir> <lz4_dir> [--n-files 10]
```

For a random sample of `--n-files` file pairs:
- Fetch every `points` blob from both source and destination.
- Assert `lz4.frame.decompress(lz4_blob[1:]) == bz2.decompress(bz2_blob)`
  for every row — i.e., the raw `Point.pack()` bytes are identical.
- Reports pass/fail per file.

This directly verifies the round-trip is lossless, independent of the query
path.

### 6. `verify_queries.py` — query-level spot check

```
python scripts/verify_queries.py <bz2_dir> <lz4_dir>
```

Runs a fixed set of representative queries (covering different spatial
regions, temporal types, and `filter_waypoints` on/off) against both
directories.  Asserts:
- Same number of results.
- Same set of `source_id`s.
- Field-for-field exact equality on every `Point` in every `Trajectory`
  (sorted by `source_id` before comparison, since SQL order is not
  guaranteed).

### 7. `hypothesis_test.py` — property-based equivalence test

```
pytest scripts/hypothesis_test.py -x -v
```

A Hypothesis `@given` test that generates random but valid `FlightQuery`
objects anchored to the actual date range of the data (so the vast majority
return non-empty results), runs each query against both the bz2 and lz4
directories, and asserts exact equality.

Query generation strategy:
- Draw a random date from the set of dates present in the data.
- Draw a random UTC hour offset within that date.
- Draw a random duration (15 min – 4 h).
- Optionally draw a random bounding box within plausible lat/lon bounds.
- Randomly select temporal query type, spatial query type, and
  `filter_waypoints`.

Comparison: sort results by `source_id`; assert equal length; assert
field-for-field equality on every `Trajectory` and every `Point`.  **No
floating-point tolerance** — the round-trip is lossless so results must be
bit-for-bit identical.

Run with `max_examples=2000` for a thorough sweep before declaring the
conversion correct.

### 8. `soak_test.py` — continuous equivalence test

```
python scripts/soak_test.py <bz2_dir> <lz4_dir>
```

A `while True` loop that continuously generates random queries (same
strategy as the Hypothesis test), runs them against both directories, and
asserts exact equality.  Logs a summary every 100 queries (count, any
failures, wall time).  Run for ~48 hours before scheduling the cutover.
Kill with Ctrl-C.

---

## Verification sequence

Run these in order after the bulk conversion completes.  Each gate must pass
before proceeding to the next.

| Step | Script | Pass criterion |
|------|--------|----------------|
| 1 | `verify_counts.py` | Zero mismatches across all 1 307 files |
| 2 | `verify_sample.py --n-files 20` | Raw bytes identical for all sampled rows |
| 3 | `verify_queries.py` | Exact match on all canned queries |
| 4 | `hypothesis_test.py` | No failures at `max_examples=2000` |
| 5 | `soak_test.py` (48 h) | Zero failures over the soak window |

Only after step 5 passes: schedule the cutover announcement.

---

## Migration procedure

### Phase 1 — bulk conversion (days to weeks before cutover)

1. Create the destination directory structure:
   ```bash
   mkdir -p /data2/feder/temp_lz4
   ```
2. Submit the Slurm job array:
   ```bash
   sbatch scripts/bulk_convert.sh /data2/feder/main /data2/feder/temp_lz4
   ```
3. Monitor job completion.  Inspect any failed task logs.  Re-run failed
   tasks individually.
4. Run the verification sequence (all 5 steps above).

### Phase 2 — tail sync (daily, from end of bulk conversion until cutover)

5. Start the daily cron (`tail_convert.sh`) to convert each new day-file as
   it closes.  Verify each converted file with `verify_counts.py` as a
   post-step.

### Phase 3 — user announcement

6. Announce the upcoming cutover with at least one week's notice.  Key
   points:
   - Feder 1.0.0 is coming with a ~10× query speedup.
   - The data format is changing; users **must** upgrade to 1.0.0.
   - Users on the old library hitting new data will see
     `OSError: Invalid data stream` — the fix is to upgrade.
   - Date and time of the maintenance window.

### Phase 4 — cutover (overnight Boston time)

7. **Stop the ingester.**
8. Identify the current partial day-files (today's, possibly yesterday's and
   tomorrow's due to the 2–3 day lag).  Convert each one:
   ```bash
   python scripts/convert_file.py \
       /data2/feder/main/YYYY/YYYY-DDD.sqlite \
       /data2/feder/temp_lz4/YYYY/YYYY-DDD.sqlite
   ```
   Run `verify_counts.py` on just these files.
9. Stop the daily cron.
10. Swap directories:
    ```bash
    mv /data2/feder/main /data2/feder/main_bz2_backup
    mv /data2/feder/temp_lz4 /data2/feder/main
    ```
11. **Restart the ingester** on the new code (1.0.0).  It will detect the
    existing partial day-file(s) in `/data2/feder/main` and append to them
    in lz4 format.
12. Smoke-test: run a query against `/data2/feder/main` using the new
    library and confirm results look correct.
13. Publish `1.0.0` to PyPI.
14. Send the cutover announcement to users.

### Phase 5 — cleanup (≥14 days after cutover)

15. If no issues have surfaced, delete the bz2 backup:
    ```bash
    rm -rf /data2/feder/main_bz2_backup
    ```

---

## Rollback plan

If a critical bug is discovered after cutover:

1. Stop the ingester.
2. Swap directories back:
   ```bash
   mv /data2/feder/main /data2/feder/main_lz4_broken
   mv /data2/feder/main_bz2_backup /data2/feder/main
   ```
3. Restart the ingester on the previous library version.
4. Yank the 1.0.0 release from PyPI (`pip install feder` falls back to
   0.2.4).
5. Notify users to downgrade: `pip install feder==0.2.4`.

Note: the bz2 backup will be missing any day-files written since cutover.
Those files would need to be re-converted from lz4 back to bz2 (or
accepted as a data gap) if rollback is necessary after more than a few
hours.  This is why the 48-hour soak test is important.

---

## Release notes (draft)

### Feder 1.0.0

**Breaking change: data format has changed.  You must upgrade.**

If you see `OSError: Invalid data stream` when running queries, you are
running an old version of Feder against the new data files.  Run
`pip install --upgrade feder` to fix it.

**What's new:**

- **~10× faster queries** for typical bounded spatial+temporal queries
  (e.g. 1-hour window, 50 km bounding box).  The speedup comes from three
  compounding changes:
  - Point data is now parsed with numpy rather than a per-point Python
    struct loop, eliminating millions of Python function calls per query.
  - Spatial crossing checks and waypoint filters are numpy boolean-mask
    operations; `Point` objects are only constructed for trajectories that
    pass all filters.
  - bz2 decompression of trajectory blobs is now parallelised across 8
    threads (bz2 releases the GIL).

- **New storage format** — trajectory point data is now compressed with
  lz4 (frame format) rather than bz2, with a version byte prefix for
  future-proof format evolution.  lz4 decompresses ~600× faster than bz2.

- **numpy** is now a dependency of `feder-common` (it was already a
  de-facto dependency for all Feder users).

- **lz4** is now a dependency of `feder-common` and `feder-ingest`.
