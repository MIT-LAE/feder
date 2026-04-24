#!/usr/bin/env python3
"""Query-level verification: run representative FlightQuery calls against
both the bz2 and lz4 stores and assert exact equality of results.

Required environment variables (see _query_harness.ComparisonHarness):

    BZ2_CODE_DIR  BZ2_DATA_DIR  LZ4_CODE_DIR  LZ4_DATA_DIR

Usage:
    python scripts/verify_queries.py
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, UTC

from _query_harness import ComparisonHarness


T1 = datetime(2025, 5, 22, 18, 0, tzinfo=UTC)
T2 = T1 + timedelta(hours=1)

_BOSTON_50KM = {
    'min_lat': 41.91, 'max_lat': 42.81,
    'min_lon': -71.50, 'max_lon': -70.61,
}
_WESTERN_US = {
    'min_lat': 30.0, 'max_lat': 50.0,
    'min_lon': -125.0, 'max_lon': -105.0,
}

QUERIES: list[tuple[str, list]] = [
    ('time_intersects, no spatial', []),
    ('time_starts_in, no spatial', [['time_starts_in', []]]),
    ('time_ends_in, no spatial', [['time_ends_in', []]]),
    ('spatially_crosses, Boston 50 km', [
        ['spatially_crosses', []],
        ['with_bounds', [_BOSTON_50KM]],
    ]),
    ('spatially_crosses + filter_waypoints, Boston 50 km', [
        ['spatially_crosses', []],
        ['with_bounds', [_BOSTON_50KM]],
        ['filter_waypoints', []],
    ]),
    ('spatially_crosses, western US', [
        ['spatially_crosses', []],
        ['with_bounds', [_WESTERN_US]],
    ]),
    ('time_starts_in + orig filter', [
        ['time_starts_in', []],
        ['with_orig', ['KPSC']],
    ]),
]


def main() -> None:
    failures = 0
    with ComparisonHarness.from_env() as h:
        for desc, ops in QUERIES:
            verdict = h.compare(T1, T2, ops)
            status = 'OK  ' if verdict.ok else 'FAIL'
            print(f'{status}  {desc}: {verdict.message}')
            if not verdict.ok:
                failures += 1

    if failures:
        print(f'\n{failures} query check(s) failed.', file=sys.stderr)
        sys.exit(1)
    print(f'\nAll {len(QUERIES)} query checks passed.')


if __name__ == '__main__':
    main()
