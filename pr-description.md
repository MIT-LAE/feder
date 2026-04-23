## Query API performance optimisation

### Motivation

The flight query API is used in a long-lived server process to load trajectory data for projecting onto ground-based camera video. A representative query — 1-hour time window, 50 km bounding box, `spatially_crosses` + `filter_waypoints` — was taking **~1.9 s**, well above the ~200 ms target needed to keep below video-stream initialisation time.

### What was changed

#### 1. Vectorised point parsing with numpy (`feder-common`)

**Root cause:** `Point.unpack` deserialised trajectory points one at a time in a Python loop, calling `struct.unpack` and `round(..., 3)` (`milli()`) per point. With ~890 K points unpacked per query call, this generated 22 M Python function calls and dominated the profile at ~76 % of wall time.

**Fix:**
- Added a `_POINT_DTYPE` numpy structured dtype matching the on-disk binary layout (`'>Lddddd?'`).
- Added `Point._unpack_blob(data)` — parses a full decompressed blob in one `numpy.frombuffer` call.
- Added `Point._array_to_points(arr)` — materialises `Point` objects only for trajectories that survive all filters.
- Replaced the Python-lambda `points_checker` (spatial crossing check) and `_make_point_filter` (waypoint filter) with numpy boolean-mask operations, so Point objects are never constructed for the ~59 % of R-tree candidates that are ultimately rejected.
- Removed `milli()` from the hot path. It was originally needed for round-tripping against an old CSV format; it is preserved in `utils.py` and remains in `tests/conftest.py` where it still makes comparisons robust.

**Result:** 1.90 s → 0.56 s (**3.4×**)

#### 2. Parallel bz2 decompression with a thread pool (`feder-common`)

**Root cause:** After the numpy change, profiling showed bz2 decompression of ~1 950 trajectory blobs was 92 % of remaining time. Decompressions were serial, and `bz2` is a C extension that releases the GIL — straightforward to parallelise with threads.

**Fix:**
- Added a module-level `ThreadPoolExecutor` (default 8 workers — empirically the sweet spot on this host; more threads increase scheduling and memory-bandwidth contention without adding throughput for this workload).
- Added `_process_blob(blob, points_check, pt_filter)` as the per-trajectory unit of work submitted to the pool: decompress → numpy parse → spatial check → waypoint filter, all running in parallel.
- Removed the `batched(ids, 50)` loop in `query_flights`: all R-tree IDs are now passed to `_retrieve` in a single SQL query (SQLite handles thousands of `IN`-clause parameters without issue), giving the pool the maximum available parallelism in one shot.

**Result:** 0.56 s → ~0.18 s median (**further 3.1×**)

### Overall results

| | Time (per call) | Speedup vs baseline |
|---|---|---|
| Baseline | 1.90 s | — |
| + numpy vectorisation | 0.56 s | 3.4× |
| + thread pool | ~0.18 s median | ~10.5× |

Measured on a warm server process (OS page cache hot, 10 repeated calls after one warm-up). Min observed: 128 ms; p90: ~275 ms (tail variance is from shared-host load, not the query path itself).

### Dependencies added

- `numpy >= 1.24` added to `feder-common` (already a de-facto dependency for all Feder users).

### What's next

The remaining ~150 ms is almost entirely bz2 decompression. Replacing bz2 with lz4 (decompresses at ~6 GB/s vs bz2's ~10 MB/s) would reduce decompression to near-zero and push all percentiles comfortably under 100 ms. That requires a data migration and ingestion-pipeline change, and is tracked separately.
