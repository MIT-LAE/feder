from datetime import datetime, date
import logging
from operator import attrgetter
import os
import sqlite3

from feder.common import DB, Trajectory, Point, MISSING_VALUE


logger = logging.getLogger(__name__)


class WritableDB(DB):
    def __init__(self, data_dir: str, ref_date: datetime | date | int):
        super().__init__(data_dir, ref_date, must_exist=False)
        if not os.path.exists(self._db_file()):
            self._create_db()

    def _create_db(self) -> None:
        logger.info('creating database file %s', self._db_file())

        # Need to make a separate connection here because this will be called
        # before the database file exists.
        conn = sqlite3.connect(self._db_file())
        cur = conn.cursor()

        cur.execute("""
          CREATE VIRTUAL TABLE IF NOT EXISTS trajectory_index USING rtree(
            id INTEGER PRIMARY KEY,
            min_timestamp, max_timestamp,
            min_latitude, max_latitude,
            min_longitude, max_longitude,
            min_altitude, max_altitude
          )""")

        cur.execute("""
          CREATE TABLE IF NOT EXISTS trajectories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source INTEGER NOT NULL,
            source_id TEXT NOT NULL,
            transponder_id TEXT NOT NULL,
            callsign TEXT NOT NULL,
            aircraft_type TEXT,
            points BLOB NOT NULL /* Protocol Buffers Points message (points.proto) */
          )""")

    def add_trajectory(self, traj: Trajectory) -> int:
        cur = self.conn.cursor()

        cur.execute(
            """INSERT INTO trajectories
            (source, source_id, transponder_id, callsign,
             aircraft_type, points)
            VALUES (?, ?, ?, ?, ?, ?) RETURNING id""",
            (traj.source.value, traj.id, traj.transponder_id, traj.callsign,
             traj.aircraft_type, Point.pack(traj.points))
        )
        id = cur.fetchone()[0]

        min_time, max_time = _time_range(traj)
        min_lat, max_lat = _lat_range(traj)
        min_lon, max_lon = _lon_range(traj)
        min_alt, max_alt = _alt_range(traj)
        cur.execute(
            """INSERT INTO trajectory_index
                 (id, min_timestamp, max_timestamp,
                  min_latitude, max_latitude,
                  min_longitude, max_longitude,
                  min_altitude, max_altitude)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (id, min_time, max_time,
             min_lat, max_lat, min_lon, max_lon, min_alt, max_alt)
        )

        self.conn.commit()
        return id


def _range(traj: Trajectory, attr: str) -> tuple:
    return (
        getattr(min(traj.points, key=attrgetter(attr)), attr),
        getattr(max(traj.points, key=attrgetter(attr)), attr)
    )


def _time_range(traj: Trajectory) -> tuple:
    return _range(traj, 'time')


def _lat_range(traj: Trajectory) -> tuple:
    return _range(traj, 'lat')


def _lon_range(traj: Trajectory) -> tuple:
    return _range(traj, 'lon')


def _alt_range(traj: Trajectory) -> tuple:
    alts = [p.alt for p in traj.points]
    if all(a is None for a in alts):
        return (MISSING_VALUE, MISSING_VALUE)
    ok_alts = [a for a in alts if a is not None]
    return min(ok_alts), max(ok_alts)
