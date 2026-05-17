from datetime import date, datetime, UTC
from pathlib import Path
import sqlite3

import lz4.frame
import numpy as np
import pytest

from feder_common import DB, DataSource, Point
from feder_common.db import _BLOB_VERSION


DATA_DIR = Path(__file__).parents[1] / 'api' / 'data'
REF_DATE = date(2025, 5, 22)


def test_stream_trajectories_returns_all_rows():
    db = DB(str(DATA_DIR), REF_DATE)
    try:
        trajectories = list(db.stream_trajectories())
        assert len(trajectories) == db.size()
        assert len(trajectories) > 0
        assert all(not traj.partial for traj in trajectories)
    finally:
        db.close()


def test_stream_trajectories_small_batch_returns_all_rows():
    db = DB(str(DATA_DIR), REF_DATE)
    try:
        assert len(list(db.stream_trajectories(batch_size=1))) == db.size()
        assert len(list(db.stream_trajectories(batch_size=2))) == db.size()
    finally:
        db.close()


@pytest.mark.parametrize('batch_size', [0, -1])
def test_stream_trajectories_rejects_invalid_batch_size(batch_size):
    db = DB(str(DATA_DIR), REF_DATE)
    try:
        with pytest.raises(ValueError, match='batch_size must be at least 1'):
            list(db.stream_trajectories(batch_size=batch_size))
    finally:
        db.close()


def test_stream_trajectory_arrays_returns_all_rows():
    db = DB(str(DATA_DIR), REF_DATE)
    try:
        trajectories = list(db.stream_trajectory_arrays())
        assert len(trajectories) == db.size()
        assert len(trajectories) > 0
        assert all(not traj.partial for traj in trajectories)
        assert all(isinstance(traj.points, np.ndarray) for traj in trajectories)
    finally:
        db.close()


def test_stream_trajectory_arrays_small_batch_returns_all_rows():
    db = DB(str(DATA_DIR), REF_DATE)
    try:
        assert len(list(db.stream_trajectory_arrays(batch_size=1))) == db.size()
    finally:
        db.close()


@pytest.mark.parametrize('batch_size', [0, -1])
def test_stream_trajectory_arrays_rejects_invalid_batch_size(batch_size):
    db = DB(str(DATA_DIR), REF_DATE)
    try:
        with pytest.raises(ValueError, match='batch_size must be at least 1'):
            list(db.stream_trajectory_arrays(batch_size=batch_size))
    finally:
        db.close()


def test_stream_trajectory_arrays_defaults_to_native_endian():
    db = DB(str(DATA_DIR), REF_DATE)
    try:
        traj = next(db.stream_trajectory_arrays())
        assert traj.points.dtype['time'].byteorder in ('=', '|')
        assert traj.points.dtype['lon'].byteorder in ('=', '|')
    finally:
        db.close()


def test_stream_trajectory_arrays_fast_mode_returns_raw_endian():
    db = DB(str(DATA_DIR), REF_DATE)
    try:
        traj = next(db.stream_trajectory_arrays(
            native_endian=False, missing_as_nan=False
        ))
        assert traj.points.dtype['time'].byteorder == '>'
        assert traj.points.dtype['lon'].byteorder == '>'
    finally:
        db.close()


def test_stream_trajectory_arrays_converts_missing_values_to_nan(tmp_path):
    day = date(2025, 1, 1)
    data_dir = tmp_path / 'data'
    db_path = data_dir / '2025' / '2025-001.sqlite'
    db_path.parent.mkdir(parents=True)
    points = [
        Point(
            time=datetime(2025, 1, 1, 0, 0, tzinfo=UTC),
            lon=-70.0, lat=40.0, alt=None, alt_gnss=None,
            heading=None, on_ground=False,
        ),
        Point(
            time=datetime(2025, 1, 1, 0, 1, tzinfo=UTC),
            lon=-71.0, lat=41.0, alt=1000.0, alt_gnss=1100.0,
            heading=90.0, on_ground=False,
        ),
    ]
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """CREATE TABLE trajectories (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              source INTEGER NOT NULL,
              source_id TEXT NOT NULL,
              transponder_id TEXT,
              orig TEXT,
              dest TEXT,
              callsign TEXT NOT NULL,
              aircraft_type TEXT,
              points BLOB NOT NULL
            )"""
        )
        conn.execute(
            """INSERT INTO trajectories
               (source, source_id, transponder_id, orig, dest, callsign,
                aircraft_type, points)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                DataSource.FLIGHTAWARE.value, 'source-id', 'ABC123', None,
                None, 'CALL', None,
                bytes([_BLOB_VERSION]) + lz4.frame.compress(Point.pack(points)),
            )
        )
        conn.commit()
    finally:
        conn.close()

    db = DB(str(data_dir), day)
    try:
        traj = next(db.stream_trajectory_arrays())
        assert np.isnan(traj.points['alt'][0])
        assert np.isnan(traj.points['alt_gnss'][0])
        assert np.isnan(traj.points['heading'][0])
        assert traj.points['alt'][1] == 1000.0
        assert traj.points['alt_gnss'][1] == 1100.0
        assert traj.points['heading'][1] == 90.0
    finally:
        db.close()
