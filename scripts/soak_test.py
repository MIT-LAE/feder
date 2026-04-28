#!/usr/bin/env python3
"""Continuous equivalence soak test: run random FlightQuery calls in a
loop and assert that bz2 and lz4 return identical results.  Intended to
run for ~48 hours before the cutover; progress is printed every 100
queries.

Required environment variables:

    BZ2_CODE_DIR  BZ2_DATA_DIR  LZ4_CODE_DIR  LZ4_DATA_DIR

Usage:
    python scripts/soak_test.py [--seed 42]
"""

from __future__ import annotations

import argparse
import math
import random
import sys
import time
from datetime import datetime, timedelta, UTC

from _query_harness import ComparisonHarness


_EPOCH = datetime(2025, 1, 1, tzinfo=UTC)
_DATA_DAYS = 365

# Bounding boxes model the primary use case: regions around ground-based
# cameras covering their effective field of view.  Width and height are
# each drawn independently from [10 km, 200 km].
_KM_PER_DEG_LAT = 111.0
_MIN_BOX_KM = 10.0
_MAX_BOX_KM = 200.0


def random_query(rng: random.Random) -> tuple[datetime, datetime, list, dict]:
    t1 = _EPOCH + timedelta(
        days=rng.randint(0, _DATA_DAYS - 1),
        hours=rng.randint(0, 23),
        minutes=rng.randint(0, 59),
    )
    t2 = t1 + timedelta(minutes=rng.randint(5, 30))

    center_lat = rng.uniform(25.0, 48.0)
    center_lon = rng.uniform(-125.0, -70.0)
    ns_km = rng.uniform(_MIN_BOX_KM, _MAX_BOX_KM)
    ew_km = rng.uniform(_MIN_BOX_KM, _MAX_BOX_KM)
    lat_half = (ns_km / 2.0) / _KM_PER_DEG_LAT
    lon_half = (ew_km / 2.0) / (_KM_PER_DEG_LAT * math.cos(math.radians(center_lat)))
    bbox = {
        'min_lat': center_lat - lat_half,
        'max_lat': center_lat + lat_half,
        'min_lon': center_lon - lon_half,
        'max_lon': center_lon + lon_half,
    }

    temporal = rng.choice([
        'time_intersects', 'time_within', 'time_starts_in', 'time_ends_in',
    ])
    filter_wp = rng.random() < 0.5

    ops: list = [
        [temporal, []],
        ['spatially_crosses', []],
        ['with_bounds', [bbox]],
    ]
    if filter_wp:
        ops.append(['filter_waypoints', []])

    params = {'temporal': temporal, 'bbox': bbox, 'filter_wp': filter_wp}
    return t1, t2, ops, params


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seed', type=int, default=None)
    args = parser.parse_args()

    rng = random.Random(args.seed)
    n_total = 0
    n_failures = 0
    t_start = time.monotonic()

    def report() -> None:
        elapsed = time.monotonic() - t_start
        rate = n_total / elapsed if elapsed > 0 else 0.0
        print(
            f'[{datetime.now().strftime("%H:%M:%S")}]  '
            f'{n_total} queries  {n_failures} failures  '
            f'{rate:.1f} q/s  elapsed {elapsed/3600:.1f}h',
            flush=True,
        )

    print('Soak test starting.  Ctrl-C to stop.\n', flush=True)

    try:
        with ComparisonHarness.from_env() as h:
            while True:
                t1, t2, ops, params = random_query(rng)
                verdict = h.compare(t1, t2, ops)
                if not verdict.ok:
                    n_failures += 1
                    print(f'FAIL  t1={t1.isoformat()} t2={t2.isoformat()} {params}')
                    print(f'      {verdict.message}')
                n_total += 1
                if n_total % 100 == 0:
                    report()
    except KeyboardInterrupt:
        pass

    report()
    sys.exit(1 if n_failures else 0)


if __name__ == '__main__':
    main()
