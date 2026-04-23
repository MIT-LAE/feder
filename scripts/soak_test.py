#!/usr/bin/env python3
"""Continuous equivalence soak test: run random FlightQuery calls in a loop
and assert that results from the bz2 and lz4 directories are identical.

Run for ~48 hours before scheduling the cutover.  Kill with Ctrl-C.
Progress is printed every 100 queries.

Usage:
    python scripts/soak_test.py <bz2_dir> <lz4_dir> [--seed 42]
"""

import argparse
import os
import random
import signal
import sys
import time
from datetime import datetime, timedelta, UTC

# ── query generation ──────────────────────────────────────────────────────────

# Adjust to the actual date range present in both directories.
_EPOCH = datetime(2025, 1, 1, tzinfo=UTC)
_DATA_DAYS = 365


def random_query_params(rng: random.Random) -> dict:
    start = _EPOCH + timedelta(
        days=rng.randint(0, _DATA_DAYS - 1),
        hours=rng.randint(0, 23),
    )
    end = start + timedelta(minutes=rng.randint(15, 240))

    use_bbox = rng.random() < 0.6
    bbox = {}
    if use_bbox:
        min_lat = rng.uniform(20.0, 48.0)
        max_lat = min_lat + rng.uniform(1.0, 10.0)
        min_lon = rng.uniform(-130.0, -75.0)
        max_lon = min_lon + rng.uniform(1.0, 15.0)
        bbox = dict(min_lat=min_lat, max_lat=max_lat,
                    min_lon=min_lon, max_lon=max_lon)

    temporal = rng.choice(['intersects', 'within', 'starts_in', 'ends_in'])
    filter_wp = use_bbox and rng.random() < 0.5

    return dict(t1=start, t2=end, bbox=bbox, temporal=temporal, filter_wp=filter_wp)


def run_query(data_dir: str, params: dict) -> list:
    os.environ['FEDER_DATA_DIR'] = data_dir
    from feder import FlightQuery

    q = FlightQuery(params['t1'], params['t2'])
    match params['temporal']:
        case 'intersects': q = q.time_intersects()
        case 'within':     q = q.time_within()
        case 'starts_in':  q = q.time_starts_in()
        case 'ends_in':    q = q.time_ends_in()

    if params['bbox']:
        q = q.spatially_crosses().with_bounds(**params['bbox'])
        if params['filter_wp']:
            q = q.filter_waypoints()

    return sorted(list(q.run()), key=lambda t: t.source_id)


def results_equal(a: list, b: list) -> tuple[bool, str]:
    if len(a) != len(b):
        return False, f'count bz2={len(a)} lz4={len(b)}'
    for ta, tb in zip(a, b):
        if ta.source_id != tb.source_id:
            return False, f'source_id mismatch {ta.source_id!r} != {tb.source_id!r}'
        if len(ta.points) != len(tb.points):
            return False, (
                f'traj {ta.source_id} point count '
                f'{len(ta.points)} != {len(tb.points)}'
            )
        for j, (pa, pb) in enumerate(zip(ta.points, tb.points)):
            if pa != pb:
                return False, f'traj {ta.source_id} point {j}: {pa} != {pb}'
    return True, ''


# ── main loop ─────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('bz2_dir', help='source bz2 data directory')
    parser.add_argument('lz4_dir', help='destination lz4 data directory')
    parser.add_argument('--seed', type=int, default=None)
    args = parser.parse_args()

    rng = random.Random(args.seed)
    n_total = 0
    n_failures = 0
    t_start = time.monotonic()

    def report():
        elapsed = time.monotonic() - t_start
        rate = n_total / elapsed if elapsed > 0 else 0
        print(
            f'[{datetime.now().strftime("%H:%M:%S")}]  '
            f'{n_total} queries  {n_failures} failures  '
            f'{rate:.1f} q/s  elapsed {elapsed/3600:.1f}h'
        )

    signal.signal(signal.SIGINT, lambda *_: (report(), sys.exit(0)))

    print(f'Soak test started.  bz2={args.bz2_dir}  lz4={args.lz4_dir}')
    print('Press Ctrl-C to stop.\n')

    while True:
        params = random_query_params(rng)
        try:
            bz2_results = run_query(args.bz2_dir, params)
            lz4_results = run_query(args.lz4_dir, params)
            ok, msg = results_equal(bz2_results, lz4_results)
            if not ok:
                n_failures += 1
                print(f'FAIL  {params}')
                print(f'      {msg}')
        except Exception as e:
            n_failures += 1
            print(f'ERROR  {params}')
            print(f'       {e}')

        n_total += 1
        if n_total % 100 == 0:
            report()


if __name__ == '__main__':
    main()
