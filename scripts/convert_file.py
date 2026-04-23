#!/usr/bin/env python3
"""Convert a single Feder SQLite database file from bz2 to lz4 (format version 0x01).

Usage:
    python scripts/convert_file.py <src.sqlite> <dst.sqlite>

Called by bulk_convert.sh as one Slurm array task per file.  Exits 0 on
success, non-zero on any failure so that Slurm marks the job failed.
"""

import argparse
import bz2
import sqlite3
import sys

import lz4.frame

BLOB_VERSION = 0x01


def convert(src_path: str, dst_path: str) -> None:
    src = sqlite3.connect(f'file:{src_path}?mode=ro', uri=True)
    dst = sqlite3.connect(dst_path)
    try:
        src.backup(dst)

        rows = dst.execute('SELECT id, points FROM trajectories').fetchall()
        dst.executemany(
            'UPDATE trajectories SET points=? WHERE id=?',
            [
                (bytes([BLOB_VERSION]) + lz4.frame.compress(bz2.decompress(row[1])), row[0])
                for row in rows
            ]
        )
        dst.commit()

        integrity = dst.execute('PRAGMA integrity_check').fetchone()[0]
        if integrity != 'ok':
            raise RuntimeError(f'integrity_check failed: {integrity}')

        src_count = src.execute('SELECT COUNT(*) FROM trajectories').fetchone()[0]
        dst_count = dst.execute('SELECT COUNT(*) FROM trajectories').fetchone()[0]
        if src_count != dst_count:
            raise RuntimeError(
                f'row count mismatch: src={src_count} dst={dst_count}'
            )

        print(f'OK  {src_path} -> {dst_path}  ({src_count} trajectories)')
    finally:
        dst.close()
        src.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('src', help='source bz2 SQLite file')
    parser.add_argument('dst', help='destination lz4 SQLite file')
    args = parser.parse_args()

    try:
        convert(args.src, args.dst)
    except Exception as e:
        print(f'FAIL  {args.src}: {e}', file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
