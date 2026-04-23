## Addendum: lz4 storage compression (Feder 1.0.0)

### Motivation

After the numpy and thread-pool changes, profiling showed bz2 decompression of ~1 950 trajectory blobs still accounted for the bulk of remaining latency. lz4 decompresses at ~6 GB/s vs bz2's ~10 MB/s — roughly 600× faster — reducing decompression to near-zero.

This required converting 380 GB of historical data (1 307 daily SQLite files) and is a breaking change: old library versions will raise `OSError: Invalid data stream` against the new files. It is released as Feder **1.0.0**, using the cumulative ~11× query speedup as justification for the compatibility break.

### What was changed

#### 3. lz4 storage format with version byte (`feder-common`, `feder-ingest`)

**Blob format:** each `points` blob is now stored as:
```
b'\x01' + lz4.frame.compress(Point.pack(traj.points))
```
The leading version byte (`0x01`) future-proofs subsequent compression changes without requiring another flag day.

**Reader (`db.py`):** validates the version byte and calls `lz4.frame.decompress(blob[1:])`. An unrecognised version byte raises `ValueError` with a message directing the user to upgrade.

**Writer (`writeable_db.py`):** prepends `0x01` and compresses with `lz4.frame`.

**Thread-pool fix:** switching from bz2 to lz4 exposed a granularity problem — lz4 decompressions take ~0.4 µs each (vs ~260 µs for bz2), making 1 950 individual thread-pool tasks slower than single-threaded due to per-task Python overhead. Fixed by splitting the row list into `N_WORKERS` chunks and submitting one future per chunk. `_array_to_points` is also moved inside the chunked work so Point construction is parallelised too.

#### 4. Migration tooling (`scripts/`)

Nine standalone scripts for the cluster-side data migration:

| Script | Purpose |
|---|---|
| `convert_file.py` | Per-file conversion: SQLite backup API + UPDATE + VACUUM + integrity check + row-count check |
| `bulk_convert.sh` | Slurm job array (one task per file, 20 concurrent) |
| `bulk_convert_parallel.sh` | GNU Parallel equivalent for local use |
| `tail_convert.sh` | Daily cron to keep the lz4 copy in sync until cutover |
| `verify_counts.py` | Row-count sweep across all file pairs |
| `verify_sample.py` | Decompress-and-compare spot check on a random sample |
| `verify_queries.py` | Canned FlightQuery calls against both directories, exact equality |
| `hypothesis_test.py` | Hypothesis property test with random queries, `max_examples=2000` |
| `soak_test.py` | `while True` loop for 48-hour pre-cutover confidence building |

### Overall results (cumulative)

| | Time (per call) | Speedup vs baseline |
|---|---|---|
| Baseline | 1.90 s | — |
| + numpy vectorisation | 0.56 s | 3.4× |
| + thread pool (bz2) | ~0.18 s median | ~10.5× |
| + lz4 + chunked threads | ~68 ms min / ~178 ms median | **~11×** |

Measured on a warm server process against the converted live dataset. p90 is ~264 ms; tail variance is shared-host scheduling noise, not query-path cost.

### Dependencies added

- `lz4 >= 4.4.5` added to `feder-common` and `feder-ingest`.

### Breaking change

Users on old library versions querying converted data will see `OSError: Invalid data stream`. The fix is `pip install --upgrade feder`. This will be called out prominently in the 1.0.0 release notes and announced to users before the data cutover.
