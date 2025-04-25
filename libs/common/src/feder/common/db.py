import bz2
from datetime import datetime, date
from enum import Enum, auto
from itertools import batched
import os
import sqlite3
from typing import Generator

from .models import DataSource, Point, Trajectory


class QueryType(Enum):
    CROSSES = auto()
    CONTAINS = auto()


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
        return os.path.join(self.data_dir, f'{yr:04d}/{yr:04d}-{doy:03d}.sqlite')

    @property
    def conn(self):
        if self._conn is not None:
            return self._conn
        f = self._db_file()
        if not os.path.exists(f):
            raise FileNotFoundError(f'database file {f} does not exist')
        self._conn = sqlite3.connect(f)
        return self._conn

    def cursor(self):
        return self.conn.cursor()

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    def get_flight_by_id(
            self, source: DataSource, source_id: str
    ) -> Trajectory | None:
        # Return a single result from _retrieve's generator.
        return next(self._retrieve(source, 'source_id = ?', source_id), None)

    def query_flights(
            self,
            min_time: datetime,
            max_time: datetime,
            min_lat: float | None = None,
            max_lat: float | None = None,
            min_lon: float | None = None,
            max_lon: float | None = None,
            min_alt: float | None = None,
            max_alt: float | None = None,
            source: DataSource | None = None,
            query_type: QueryType = QueryType.CROSSES
    ) -> Generator[Trajectory, None, None]:
        if max_time < min_time:
            raise ValueError('flight query max_time must be larger than min_time')
        if min_lat is not None and max_lat is not None and max_lat < min_lat:
            raise ValueError('flight query max_lat must be larger than min_lat')
        if min_lon is not None and max_lon is not None and max_lon < min_lon:
            raise ValueError('flight query max_lon must be larger than min_lon')
        if min_alt is not None and max_alt is not None and max_alt < min_alt:
            raise ValueError('flight query max_alt must be larger than min_alt')
        if query_type == QueryType.CONTAINS:
            if (min_lat is not None) != (max_lat is not None):
                raise ValueError(
                    'both or no latitude bounds needed for a contains query'
                )
            if (min_lon is not None) != (max_lon is not None):
                raise ValueError(
                    'both or no longitude bounds needed for a contains query'
                )
            if (min_alt is not None) != (max_alt is not None):
                raise ValueError(
                    'both or no altitude bounds needed for a contains query'
                )

        id_conditions = []
        id_conditions.append(('max_timestamp >= ?', min_time.timestamp()))
        id_conditions.append(('min_timestamp <= ?', max_time.timestamp()))
        if query_type == QueryType.CROSSES:
            if min_lat is not None:
                id_conditions.append(('max_latitude >= ?', min_lat))
            if max_lat is not None:
                id_conditions.append(('min_latitude <= ?', max_lat))
            if min_lon is not None:
                id_conditions.append(('max_longitude >= ?', min_lon))
            if max_lon is not None:
                id_conditions.append(('min_longitude <= ?', max_lon))
            if min_alt is not None:
                id_conditions.append(('max_altitude >= ?', min_alt))
            if max_alt is not None:
                id_conditions.append(('min_altitude <= ?', max_alt))
        else:
            if min_lat is not None and max_lat is not None:
                id_conditions.append(('min_latitude >= ?', min_lat))
                id_conditions.append(('max_latitude <= ?', max_lat))
            if min_lon is not None and max_lon is not None:
                id_conditions.append(('min_longitude >= ?', min_lon))
                id_conditions.append(('max_longitude <= ?', max_lon))
            if min_alt is not None and max_alt is not None:
                id_conditions.append(('min_altitude >= ?', min_alt))
                id_conditions.append(('max_altitude <= ?', max_alt))
        id_sql = (
            'SELECT id FROM trajectory_index WHERE ' +
            ' AND '.join(p[0] for p in id_conditions)
        )
        id_parameters = tuple(p[1] for p in id_conditions)

        cur = self.cursor()
        cur.execute(id_sql, id_parameters)
        ids = [t[0] for t in cur.fetchall()]

        # Batch the IDs to keep the length of SQL queries reasonable.
        for id_batch in batched(ids, 50):
            yield from self._retrieve(
                source,
                f'id IN ({",".join("?" for i in id_batch)})',
                list(id_batch)
            )

    def _retrieve(
            self,
            source: DataSource | None,
            id_condition: str,
            id_parameters: str | list[str]
    ) -> Generator[Trajectory, None, None]:
        # Normalize ID list parameter.
        if not isinstance(id_parameters, list):
            id_parameters = [id_parameters]

        # Basic query body.
        sql = """SELECT source, source_id, transponder_id, orig, dest,
                        callsign, aircraft_type, points
                   FROM trajectories WHERE """

        parameters = []
        if source is not None:
            # If a source is specified, add the condition and the source
            # parameter.
            sql += 'source = ? AND '
            parameters.append(source.value)

        # Add the supplied ID condition and parameters.
        sql += id_condition
        parameters += id_parameters

        cur = self.cursor()
        cur.execute(sql, parameters)

        for traj_rec in cur:
            traj = Trajectory(
                source=DataSource(traj_rec[0]), source_id=traj_rec[1],
                transponder_id=traj_rec[2], orig=traj_rec[3], dest=traj_rec[4],
                callsign=traj_rec[5], aircraft_type=traj_rec[6],
                points=Point.unpack(bz2.decompress(traj_rec[7]))
            )
            yield traj
