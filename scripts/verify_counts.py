#!/usr/bin/env python3
"""Verify trajectory row counts match between the bz2 and lz4 data directories.

Runs in minutes across all files.  Exits non-zero if any mismatch is found.

Usage:
    python scripts/verify_counts.py <bz2_dir> <lz4_dir>
"""

import argparse
import sqlite3
import sys
from pathlib import Path


def count_trajectories(path: Path) -> int:
    conn = sqlite3.connect(f'file:{path}?mode=ro', uri=True)
    try:
        return conn.execute('SELECT COUNT(*) FROM trajectories').fetchone()[0]
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('bz2_dir', help='source bz2 data directory')
    parser.add_argument('lz4_dir', help='destination lz4 data directory')
    args = parser.parse_args()

    bz2_dir = Path(args.bz2_dir)
    lz4_dir = Path(args.lz4_dir)

    bz2_files = sorted(bz2_dir.rglob('*.sqlite'))
    if not bz2_files:
        print(f'No SQLite files found in {bz2_dir}', file=sys.stderr)
        sys.exit(1)

    failures = []
    missing = []

    for bz2_file in bz2_files:
        lz4_file = lz4_dir / bz2_file.relative_to(bz2_dir)
        if not lz4_file.exists():
            missing.append(str(lz4_file))
            continue
        bz2_count = count_trajectories(bz2_file)
        lz4_count = count_trajectories(lz4_file)
        if bz2_count != lz4_count:
            failures.append(
                f'  {bz2_file.name}: bz2={bz2_count} lz4={lz4_count}'
            )

    total = len(bz2_files)
    converted = total - len(missing)

    print(f'Checked {converted}/{total} files.')

    if missing:
        print(f'\nNot yet converted ({len(missing)} files):')
        for f in missing[:20]:
            print(f'  {f}')
        if len(missing) > 20:
            print(f'  ... and {len(missing) - 20} more')

    if failures:
        print(f'\nRow count mismatches ({len(failures)} files):')
        for f in failures:
            print(f)
        sys.exit(1)

    if not missing:
        print('All row counts match.')


if __name__ == '__main__':
    main()
