#!/usr/bin/env python3
"""Measure public numpy-array trajectory streaming throughput for one SQLite day file.

Usage:
    uv run --package feder python scripts/stream_performance.py \
        /data/feder/2026/2026-091.sqlite
"""

import argparse
from datetime import datetime
import os
from pathlib import Path
import sys
from time import perf_counter

from feder import stream_trajectory_arrays


def day_from_db_file(path: Path):
    try:
        return datetime.strptime(path.stem, '%Y-%j').date()
    except ValueError as exc:
        raise ValueError(
            f'database filename must look like YYYY-DOY.sqlite: {path.name}'
        ) from exc


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        'database_file',
        type=Path,
        help='SQLite day file to stream, e.g. /data/feder/2026/2026-091.sqlite',
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='trajectory row batch size for stream_trajectory_arrays (default: 1000)',
    )
    parser.add_argument(
        '--raw-arrays',
        action='store_true',
        help='use raw big-endian arrays and leave missing-value sentinels unchanged',
    )
    args = parser.parse_args()

    db_file = args.database_file.resolve()
    if not db_file.exists():
        print(f'database file does not exist: {db_file}', file=sys.stderr)
        sys.exit(1)

    day = day_from_db_file(db_file)
    data_dir = db_file.parent.parent
    os.environ['FEDER_DATA_DIR'] = str(data_dir)

    trajectory_count = 0
    waypoint_count = 0

    started = perf_counter()
    for trajectory in stream_trajectory_arrays(
            day,
            batch_size=args.batch_size,
            native_endian=not args.raw_arrays,
            missing_as_nan=not args.raw_arrays,
    ):
        trajectory_count += 1
        waypoint_count += len(trajectory.points)
        if trajectory_count % args.batch_size == 0:
            print(f'processed {trajectory_count} trajectories...', file=sys.stderr)
    elapsed = perf_counter() - started

    print(f'database_file: {db_file}')
    print(f'day: {day.isoformat()}')
    print(f'batch_size: {args.batch_size}')
    print(f'raw_arrays: {args.raw_arrays}')
    print(f'trajectories: {trajectory_count}')
    print(f'waypoints: {waypoint_count}')
    print(f'elapsed_seconds: {elapsed:.3f}')
    if elapsed > 0:
        print(f'trajectories_per_second: {trajectory_count / elapsed:.1f}')
        print(f'waypoints_per_second: {waypoint_count / elapsed:.1f}')


if __name__ == '__main__':
    main()
