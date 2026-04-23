#!/usr/bin/env python3
"""Query-level verification: run representative FlightQuery calls against
both the bz2 and lz4 directories and assert exact equality of results.

Run after verify_sample.py passes.

Usage:
    python scripts/verify_queries.py <bz2_dir> <lz4_dir>
"""

import argparse
import os
import sys
from datetime import datetime, timedelta, UTC

# Queries are run by temporarily pointing FEDER_DATA_DIR at each directory.
# Import feder after setting the env var in each helper.

QUERIES = [
    # (description, builder_fn)
    # Each builder_fn receives (t1, t2) and returns a FlightQuery.
    ('time_intersects, no spatial',
     lambda t1, t2: __import__('feder').FlightQuery(t1, t2)),
    ('time_starts_in, no spatial',
     lambda t1, t2: __import__('feder').FlightQuery(t1, t2).time_starts_in()),
    ('time_ends_in, no spatial',
     lambda t1, t2: __import__('feder').FlightQuery(t1, t2).time_ends_in()),
    ('spatially_crosses, Boston 50 km',
     lambda t1, t2: (
         __import__('feder').FlightQuery(t1, t2)
         .spatially_crosses()
         .with_bounds(min_lat=41.91, max_lat=42.81, min_lon=-71.50, max_lon=-70.61)
     )),
    ('spatially_crosses + filter_waypoints, Boston 50 km',
     lambda t1, t2: (
         __import__('feder').FlightQuery(t1, t2)
         .spatially_crosses()
         .with_bounds(min_lat=41.91, max_lat=42.81, min_lon=-71.50, max_lon=-70.61)
         .filter_waypoints()
     )),
    ('spatially_crosses, western US',
     lambda t1, t2: (
         __import__('feder').FlightQuery(t1, t2)
         .spatially_crosses()
         .with_bounds(min_lat=30.0, max_lat=50.0, min_lon=-125.0, max_lon=-105.0)
     )),
    ('time_starts_in + orig filter',
     lambda t1, t2: (
         __import__('feder').FlightQuery(t1, t2)
         .time_starts_in()
         .with_orig('KPSC')
     )),
]

# A known time window with data.
T1 = datetime(2025, 5, 22, 18, 0, tzinfo=UTC)
T2 = T1 + timedelta(hours=1)


def run_query(data_dir: str, builder_fn) -> list:
    os.environ['FEDER_DATA_DIR'] = data_dir
    import importlib
    import feder
    importlib.reload(feder)
    q = builder_fn(T1, T2)
    results = sorted(list(q.run()), key=lambda t: t.source_id)
    return results


def trajectories_equal(a, b) -> tuple[bool, str]:
    if len(a) != len(b):
        return False, f'result count differs: bz2={len(a)} lz4={len(b)}'
    for i, (ta, tb) in enumerate(zip(a, b)):
        if ta.source_id != tb.source_id:
            return False, f'trajectory {i}: source_id {ta.source_id!r} != {tb.source_id!r}'
        if len(ta.points) != len(tb.points):
            return False, (
                f'trajectory {ta.source_id}: '
                f'point count {len(ta.points)} != {len(tb.points)}'
            )
        for j, (pa, pb) in enumerate(zip(ta.points, tb.points)):
            if pa != pb:
                return False, (
                    f'trajectory {ta.source_id} point {j}: {pa} != {pb}'
                )
    return True, f'{len(a)} trajectories, all fields identical'


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('bz2_dir', help='source bz2 data directory')
    parser.add_argument('lz4_dir', help='destination lz4 data directory')
    args = parser.parse_args()

    failures = 0
    for desc, builder_fn in QUERIES:
        bz2_results = run_query(args.bz2_dir, builder_fn)
        lz4_results = run_query(args.lz4_dir, builder_fn)
        ok, msg = trajectories_equal(bz2_results, lz4_results)
        status = 'OK  ' if ok else 'FAIL'
        print(f'{status}  {desc}: {msg}')
        if not ok:
            failures += 1

    if failures:
        print(f'\n{failures} query check(s) failed.', file=sys.stderr)
        sys.exit(1)
    else:
        print(f'\nAll {len(QUERIES)} query checks passed.')


if __name__ == '__main__':
    main()
