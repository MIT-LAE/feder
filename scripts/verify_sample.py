#!/usr/bin/env python3
"""Decompress-and-compare spot check: verify the raw Point.pack bytes are
identical between the bz2 and lz4 versions of a random sample of files.

This checks the round-trip directly:
    lz4.frame.decompress(lz4_blob[1:]) == bz2.decompress(bz2_blob)

Run after verify_counts.py passes.

Usage:
    python scripts/verify_sample.py <bz2_dir> <lz4_dir> [--n-files 10]
"""

import argparse
import bz2
import random
import sqlite3
import sys
from pathlib import Path

import lz4.frame

BLOB_VERSION = 0x01


def verify_file(bz2_path: Path, lz4_path: Path) -> tuple[bool, str]:
    bz2_conn = sqlite3.connect(f'file:{bz2_path}?mode=ro', uri=True)
    lz4_conn = sqlite3.connect(f'file:{lz4_path}?mode=ro', uri=True)
    try:
        bz2_rows = {
            row[0]: row[1]
            for row in bz2_conn.execute('SELECT id, points FROM trajectories')
        }
        lz4_rows = {
            row[0]: row[1]
            for row in lz4_conn.execute('SELECT id, points FROM trajectories')
        }

        if set(bz2_rows) != set(lz4_rows):
            return False, f'ID sets differ: {len(bz2_rows)} bz2 vs {len(lz4_rows)} lz4 rows'

        for row_id, bz2_blob in bz2_rows.items():
            lz4_blob = lz4_rows[row_id]
            if lz4_blob[0] != BLOB_VERSION:
                return False, f'row {row_id}: unexpected version byte {lz4_blob[0]:#04x}'
            raw_bz2 = bz2.decompress(bz2_blob)
            raw_lz4 = lz4.frame.decompress(lz4_blob[1:])
            if raw_bz2 != raw_lz4:
                return False, (
                    f'row {row_id}: decompressed bytes differ '
                    f'(bz2={len(raw_bz2)}B lz4={len(raw_lz4)}B)'
                )

        return True, f'{len(bz2_rows)} rows OK'
    finally:
        bz2_conn.close()
        lz4_conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('bz2_dir', help='source bz2 data directory')
    parser.add_argument('lz4_dir', help='destination lz4 data directory')
    parser.add_argument('--n-files', type=int, default=10,
                        help='number of files to sample (default: 10)')
    parser.add_argument('--seed', type=int, default=None,
                        help='random seed for reproducibility')
    args = parser.parse_args()

    bz2_dir = Path(args.bz2_dir)
    lz4_dir = Path(args.lz4_dir)

    all_files = sorted(bz2_dir.rglob('*.sqlite'))
    if not all_files:
        print(f'No SQLite files found in {bz2_dir}', file=sys.stderr)
        sys.exit(1)

    rng = random.Random(args.seed)
    sample = rng.sample(all_files, min(args.n_files, len(all_files)))

    failures = 0
    for bz2_path in sorted(sample):
        lz4_path = lz4_dir / bz2_path.relative_to(bz2_dir)
        if not lz4_path.exists():
            print(f'SKIP  {bz2_path.name}  (not yet converted)')
            continue
        ok, msg = verify_file(bz2_path, lz4_path)
        status = 'OK  ' if ok else 'FAIL'
        print(f'{status}  {bz2_path.name}  {msg}')
        if not ok:
            failures += 1

    if failures:
        print(f'\n{failures} file(s) failed verification.', file=sys.stderr)
        sys.exit(1)
    else:
        print(f'\nAll {len(sample)} sampled files verified.')


if __name__ == '__main__':
    main()
