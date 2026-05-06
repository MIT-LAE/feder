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


def _trajectory_for(day: datetime, source_id: str = 'source-1'):
    traj = _trajectory()
    return Trajectory(
        source=traj.source,
        source_id=source_id,
        transponder_id=traj.transponder_id,
        orig=traj.orig,
        dest=traj.dest,
        callsign=traj.callsign,
        aircraft_type=traj.aircraft_type,
        points=[Point(
            time=day.replace(hour=12, tzinfo=timezone.utc),
            lon=-71.0,
            lat=42.0,
            alt=30000,
            alt_gnss=None,
            heading=None,
            on_ground=False,
        )],
    )


def _count_rows(path):
    conn = sqlite3.connect(path)
    try:
        return conn.execute('SELECT COUNT(*) FROM trajectories').fetchone()[0]
    finally:
        conn.close()


def _make_db(root, day, *source_ids):
    db = WritableDB(str(root), day)
    try:
        for source_id in source_ids:
            db.add_trajectory(_trajectory_for(day, source_id))
    finally:
        db.close()


def _make_public_snapshot(data, staging, scratch, day, *source_ids):
    cache = DBCache(str(data), str(staging), str(scratch))
    db = WritableDB(str(staging), day)
    try:
        for source_id in source_ids:
            db.add_trajectory(_trajectory_for(day, source_id))
        cache._export_db(db, day)
    finally:
        db.close()
        cache.close()
    cache = DBCache(str(data), str(staging), str(scratch))
    try:
        cache._delete_staging_files(day)
    finally:
        cache.close()


def test_connect_creates_nursery_only_when_no_staging_or_public_exists(tmp_path):
    data = tmp_path / 'data'
    staging = tmp_path / 'staging'
    scratch = tmp_path / 'scratch'
    data.mkdir()
    cache = DBCache(str(data), str(staging), str(scratch))

    try:
        db = cache.connect(datetime(2025, 5, 22))
        assert db.in_memory
        assert datetime(2025, 5, 22) in cache._nursery
        assert not (staging / '2025' / '2025-142.sqlite').exists()
        assert not (data / '2025' / '2025-142.sqlite').exists()
    finally:
        cache.close()


def test_existing_staging_takes_precedence_over_public(tmp_path):
    day = datetime(2025, 5, 22)
    data = tmp_path / 'data'
    staging = tmp_path / 'staging'
    scratch = tmp_path / 'scratch'
    data.mkdir()
    _make_public_snapshot(data, staging, scratch, day, 'public')
    _make_db(staging, day, 'staging-1', 'staging-2')

    cache = DBCache(str(data), str(staging), str(scratch))
    try:
        db = cache.connect(day)
        assert not db.in_memory
        assert db.data_dir == str(staging)
        assert db.size() == 2
        assert _count_rows(data / '2025' / '2025-142.sqlite') == 1
    finally:
        cache.close()


def test_existing_public_imports_to_staging_and_writes_only_staging(tmp_path):
    day = datetime(2025, 5, 22)
    data = tmp_path / 'data'
    staging = tmp_path / 'staging'
    scratch = tmp_path / 'scratch'
    data.mkdir()
    _make_public_snapshot(data, staging, scratch, day, 'public')

    public_path = data / '2025' / '2025-142.sqlite'
    cache = DBCache(str(data), str(staging), str(scratch))
    try:
        db = cache.connect(day)
        assert db.data_dir == str(staging)
        assert (staging / '2025' / '2025-142.sqlite').exists()
        assert db.size() == 1

        db.add_trajectory(_trajectory_for(day, 'staging-only'))
        assert db.size() == 2
        assert _count_rows(public_path) == 1
        assert not public_path.with_name('2025-142.sqlite-wal').exists()
        assert not public_path.with_name('2025-142.sqlite-shm').exists()
    finally:
        cache.close()


def test_nursery_checkpoint_exports_public_without_creating_staging(tmp_path):
    day = datetime(2025, 5, 22)
    data = tmp_path / 'data'
    staging = tmp_path / 'staging'
    scratch = tmp_path / 'scratch'
    data.mkdir()
    cache = DBCache(str(data), str(staging), str(scratch))

    try:
        cache.add_trajectory(_trajectory_for(day, 'nursery'))
        cache.checkpoint()
        public_path = data / '2025' / '2025-142.sqlite'
        assert public_path.exists()
        assert _count_rows(public_path) == 1
        assert not (staging / '2025' / '2025-142.sqlite').exists()
    finally:
        cache.close()


def test_end_of_day_promotes_nursery_to_staging_and_publishes(tmp_path):
    day = datetime(2025, 5, 22)
    data = tmp_path / 'data'
    staging = tmp_path / 'staging'
    scratch = tmp_path / 'scratch'
    data.mkdir()
    cache = DBCache(str(data), str(staging), str(scratch))

    try:
        cache.add_trajectory(_trajectory_for(day, 'nursery'))
        cache.end_of_day(day)
        staging_path = staging / '2025' / '2025-142.sqlite'
        public_path = data / '2025' / '2025-142.sqlite'
        assert staging_path.exists()
        assert public_path.exists()
        assert _count_rows(staging_path) == 1
        assert _count_rows(public_path) == 1
    finally:
        cache.close()


def test_close_promotes_nursery_to_staging_without_deleting_it(tmp_path):
    day = datetime(2025, 5, 22)
    data = tmp_path / 'data'
    staging = tmp_path / 'staging'
    scratch = tmp_path / 'scratch'
    data.mkdir()
    cache = DBCache(str(data), str(staging), str(scratch))

    cache.add_trajectory(_trajectory_for(day, 'nursery'))
    cache.close()

    staging_path = staging / '2025' / '2025-142.sqlite'
    public_path = data / '2025' / '2025-142.sqlite'
    assert staging_path.exists()
    assert public_path.exists()
    assert _count_rows(staging_path) == 1
