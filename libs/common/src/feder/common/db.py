from dataclasses import dataclass
from datetime import datetime, date
from enum import Enum, auto
import os
import sqlite3
from typing import Generator

from .models import DataSource, Point, Trajectory


class QueryType(Enum):
    CROSSES = auto()
    CONTAINS = auto()


@dataclass
class FlightQuery:
    min_time: datetime
    max_time: datetime
    min_lat: float | None = None
    max_lat: float | None = None
    min_lon: float | None = None
    max_lon: float | None = None
    min_alt: float | None = None
    max_alt: float | None = None
    query_type: QueryType = QueryType.CROSSES


    def __post_init__(self):
        if self.max_time < self.min_time:
            raise ValueError('FlightQuery max_time must be larger than min_time')
        if (
                self.min_lat is not None and
                self.max_lat is not None and
                self.max_lat < self.min_lat
        ):
            raise ValueError('FlightQuery max_lat must be larger than min_lat')
        if (
                self.min_lon is not None and
                self.max_lon is not None and
                self.max_lon < self.min_lon
        ):
            raise ValueError('FlightQuery max_lon must be larger than min_lon')
        if (
                self.min_alt is not None and
                self.max_alt is not None and
                self.max_alt < self.min_alt
        ):
            raise ValueError('FlightQuery max_alt must be larger than min_alt')
        if self.query_type == QueryType.CONTAINS:
            if (self.min_lat is not None) != (self.max_lat is not None):
                raise ValueError(
                    'both or no latitude bounds needed for a contains query'
                )
            if (self.min_lon is not None) != (self.max_lon is not None):
                raise ValueError(
                    'both or no longitude bounds needed for a contains query'
                )
            if (self.min_alt is not None) != (self.max_alt is not None):
                raise ValueError(
                    'both or no altitude bounds needed for a contains query'
                )


class DB:
    def __init__(
            self,
            data_dir: str,
            ref_date: datetime | date | int,
            must_exist: bool = True
    ):
        self.data_dir = data_dir
        if isinstance(ref_date, int):
            ref_date = datetime.fromtimestamp(ref_date)
        if isinstance(ref_date, datetime):
            ref_date = ref_date.date()
        self.ref_date = ref_date
        if must_exist and not os.path.exists(self._db_file()):
            raise FileNotFoundError(
                f'database file {self._db_file()} does not exist'
            )
        self._conn = None

    def _db_file(self):
        yr = self.ref_date.year
        doy = self.ref_date.timetuple().tm_yday
        return os.path.join(self.data_dir, f'{yr:04d}-{doy:03d}.sqlite')

    @property
    def conn(self):
        f = self._db_file()
        if not os.path.exists(f):
            raise FileNotFoundError(f'database file {f} does not exist')
        if self._conn is None:
            self._conn = sqlite3.connect(f)
        return self._conn

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    def query_flights(self, query) -> Generator[Trajectory, None, None]:
        print(query)
        id_conditions = []
        id_conditions.append(('max_timestamp >= ?', query.min_time.timestamp()))
        id_conditions.append(('min_timestamp <= ?', query.max_time.timestamp()))
        if query.query_type == QueryType.CROSSES:
            if query.min_lat is not None:
                id_conditions.append(('max_latitude >= ?', query.min_lat))
            if query.max_lat is not None:
                id_conditions.append(('min_latitude <= ?', query.max_lat))
            if query.min_lon is not None:
                id_conditions.append(('max_longitude >= ?', query.min_lon))
            if query.max_lon is not None:
                id_conditions.append(('min_longitude <= ?', query.max_lon))
            if query.min_alt is not None:
                id_conditions.append(('max_altitude >= ?', query.min_alt))
            if query.max_alt is not None:
                id_conditions.append(('min_altitude <= ?', query.max_alt))
        else:
            if query.min_lat is not None and query.max_lat is not None:
                id_conditions.append(('min_latitude >= ?', query.min_lat))
                id_conditions.append(('max_latitude <= ?', query.max_lat))
            if query.min_lon is not None and query.max_lon is not None:
                id_conditions.append(('min_longitude >= ?', query.min_lon))
                id_conditions.append(('max_longitude <= ?', query.max_lon))
            if query.min_alt is not None and query.max_alt is not None:
                id_conditions.append(('min_altitude >= ?', query.min_alt))
                id_conditions.append(('max_altitude <= ?', query.max_alt))
        id_sql = (
            'SELECT id FROM trajectory_index WHERE ' +
            ' AND '.join(p[0] for p in id_conditions)
        )
        id_parameters = tuple(p[1] for p in id_conditions)
        print(f'id_sql = {id_sql}')
        print(f'id_parameters = {id_parameters}')

        cur = self.conn.cursor()
        cur.execute(id_sql, id_parameters)
        ids = [t[0] for t in cur.fetchall()]
        id_placeholders = ','.join('?' for i in ids)

        cur.execute(
            """SELECT source, source_id, transponder_id,
                      callsign, aircraft_type, points
                 FROM trajectories WHERE id IN (%s)""" % id_placeholders,
            ids
        )
        for traj_rec in cur:
            points = Point.unpack(traj_rec[5])
            pts = list(map(
                Point,
                points.time, points.lat, points.lon, points.alt,
                points.alt_gnss, points.on_ground
            ))
            traj = Trajectory(
                source=DataSource(traj_rec[0]), id=traj_rec[1],
                transponder_id=traj_rec[2],
                callsign=traj_rec[3], aircraft_type=traj_rec[4], points=pts
            )
            yield traj
