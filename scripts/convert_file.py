#!/usr/bin/env python3
"""Convert a single Feder SQLite database file from bz2 to lz4 (format version 0x01).

Usage:
    python scripts/convert_file.py <src.sqlite> <dst.sqlite>

Called by bulk_convert.sh as one Slurm array task per file.  Exits 0 on
success, non-zero on any failure so that Slurm marks the job failed.
"""

import argparse
import bz2
import os
import shutil
import sqlite3
import sys
import tempfile

import lz4.frame

BLOB_VERSION = 0x01


def convert(src_path: str, dst_path: str) -> None:
    # The source directory may not be writable by the user running this
    # script (SQLite needs to create -journal/-wal files alongside the DB
    # even for read-only opens in some configurations, and .backup() can be
    # slow for large files).  The source database is guaranteed to have
    # been vacuumed with no concurrent writers, so a plain file copy into a
    # writable temporary directory is safe and substantially faster.
    tmp_dir = tempfile.mkdtemp(prefix='feder-convert-')
    tmp_src_path = os.path.join(tmp_dir, os.path.basename(src_path))
    try:
        shutil.copyfile(src_path, tmp_src_path)
        shutil.copyfile(tmp_src_path, dst_path)
    except Exception:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise

    src = sqlite3.connect(f'file:{tmp_src_path}?mode=ro', uri=True)
    dst = sqlite3.connect(dst_path)
    try:
        rows = dst.execute('SELECT id, points FROM trajectories').fetchall()
        dst.executemany(
            'UPDATE trajectories SET points=? WHERE id=?',
            [
                (bytes([BLOB_VERSION]) + lz4.frame.compress(bz2.decompress(row[1])), row[0])
                for row in rows
            ]
        )
        dst.commit()

        # Reclaim free pages left by replacing smaller bz2 blobs with larger
        # lz4 blobs.  Must run outside any transaction.
        dst.isolation_level = None
        dst.execute('VACUUM')
        dst.isolation_level = ''

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
        shutil.rmtree(tmp_dir, ignore_errors=True)


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
