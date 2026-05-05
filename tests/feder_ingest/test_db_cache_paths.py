from datetime import datetime, timezone
import os
import sqlite3

import pytest

from feder_common import DataSource, Point, Trajectory
from feder_ingest import DBCache
from feder_ingest.writeable_db import WritableDB


def test_db_cache_requires_distinct_unnested_path_roots(tmp_path):
    data = tmp_path / 'data'
    data.mkdir()

    with pytest.raises(ValueError):
        DBCache(str(data), str(data), str(tmp_path / 'scratch'))

    with pytest.raises(ValueError):
        DBCache(str(data), str(tmp_path / 'staging'), str(data / 'scratch'))


def test_db_cache_requires_existing_data_dir_and_creates_private_dirs(tmp_path):
    data = tmp_path / 'data'
    staging = tmp_path / 'staging'
    scratch = tmp_path / 'scratch'

    with pytest.raises(ValueError):
        DBCache(str(data), str(staging), str(scratch))

    data.mkdir()
    db = DBCache(str(data), str(staging), str(scratch))
    try:
        assert staging.is_dir()
        assert (scratch / 'ingester-export').is_dir()
    finally:
        db.close()


def _trajectory():
    return Trajectory(
        source=DataSource.FLIGHTAWARE,
        source_id='source-1',
        transponder_id='ABCDEF',
        orig='DUMA',
        dest='DUMZ',
        callsign='TEST123',
        aircraft_type=None,
        points=[Point(
            time=datetime(2025, 5, 22, 12, 0, tzinfo=timezone.utc),
            lon=-71.0,
            lat=42.0,
            alt=30000,
            alt_gnss=None,
            heading=None,
            on_ground=False,
        )],
    )


def test_export_db_publishes_clean_immutable_snapshot(tmp_path):
    data = tmp_path / 'data'
    staging = tmp_path / 'staging'
    scratch = tmp_path / 'scratch'
    data.mkdir()
    cache = DBCache(str(data), str(staging), str(scratch))
    db = WritableDB(str(staging), datetime(2025, 5, 22))

    try:
        db.add_trajectory(_trajectory())
        cache._export_db(db, datetime(2025, 5, 22))

        public_path = data / '2025' / '2025-142.sqlite'
        assert public_path.exists()
        assert not public_path.with_name('2025-142.sqlite-wal').exists()
        assert not public_path.with_name('2025-142.sqlite-shm').exists()
        assert list((scratch / 'ingester-export').iterdir()) == []

        conn = sqlite3.connect(f'file:{public_path}?mode=ro&immutable=1', uri=True)
        try:
            count = conn.execute('SELECT COUNT(*) FROM trajectories').fetchone()[0]
            assert count == 1
            assert conn.execute('PRAGMA journal_mode').fetchone()[0] == 'delete'
        finally:
            conn.close()
    finally:
        db.close()
        cache.close()


def test_publish_snapshot_uses_hidden_temp_and_cleans_up_on_failure(tmp_path, monkeypatch):
    data = tmp_path / 'data'
    staging = tmp_path / 'staging'
    scratch = tmp_path / 'scratch'
    data.mkdir()
    cache = DBCache(str(data), str(staging), str(scratch))
    snapshot = tmp_path / 'snapshot.sqlite'
    snapshot.write_bytes(b'snapshot')
    hidden_paths = []

    def fail_replace(src, dst):
        hidden_paths.append(src)
        raise RuntimeError('replace failed')

    monkeypatch.setattr(os, 'replace', fail_replace)

    try:
        with pytest.raises(RuntimeError):
            cache._publish_snapshot(str(snapshot), datetime(2025, 5, 22))

        assert len(hidden_paths) == 1
        hidden = hidden_paths[0]
        assert os.path.basename(hidden).startswith('.2025-142.sqlite.exporting.')
        assert not hidden.endswith('.sqlite')
        assert not os.path.exists(hidden)
        assert not (data / '2025' / '2025-142.sqlite').exists()
    finally:
        cache.close()
