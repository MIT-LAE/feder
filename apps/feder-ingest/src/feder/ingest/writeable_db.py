import bz2
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

    def commit(self) -> None:
        self.conn.commit()

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
            orig TEXT,
            dest TEXT,
            callsign TEXT NOT NULL,
            aircraft_type TEXT,
            points BLOB NOT NULL /* Packed point data. */
          )""")

    def add_trajectory(
            self,
            traj: Trajectory,
            cursor: sqlite3.Cursor | None = None
    ) -> int:
        commit = cursor is not None
        cur = self.cursor() if cursor is None else cursor

        cur.execute(
            """INSERT INTO trajectories
            (source, source_id, transponder_id, orig, dest, callsign,
             aircraft_type, points)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?) RETURNING id""",
            (traj.source.value, traj.source_id, traj.transponder_id,
             traj.orig, traj.dest, traj.callsign, traj.aircraft_type,
             bz2.compress(Point.pack(traj.points)))
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
            (id, min_time.timestamp(), max_time.timestamp(),
             min_lat, max_lat, min_lon, max_lon, min_alt, max_alt)
        )

        if commit:
            self.conn.commit()
        return id

    def delete_trajectory(
            self,
            traj: Trajectory,
            cursor: sqlite3.Cursor | None = None
    ) -> None:
        commit = cursor is not None
        cur = self.cursor() if cursor is None else cursor

        cur.execute(
            """DELETE FROM trajectories
                WHERE source = ? AND source_id = ?
                RETURNING id""",
            (traj.source.value, traj.source_id)
        )
        idx_id = cur.fetchone()[0]
        cur.execute('DELETE FROM trajectory_index WHERE id = ?', (idx_id,))
        if commit:
            self.conn.commit()


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
