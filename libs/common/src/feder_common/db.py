"""Feder database access. Most of the contents of this module are for internal
use only, but the `QueryType` enumeration is needed for setting the spatial
overlap characteristics of queries."""

import bz2
from datetime import datetime, date
from enum import Enum, auto
from itertools import batched
import os
import sqlite3
from typing import Generator

from .models import DataSource, Point, Trajectory


class QueryType(Enum):
    """Spatial query types."""
    CROSSES = auto()
    """Find trajectories that cross a bounding box."""
    CONTAINS = auto()
    """Find trajectories contained within a bounding box."""


class DB:
    """Database access class. @private"""
    def __init__(
            self,
            data_dir: str,
            ref_date: datetime | date | int,
            must_exist: bool = True,
            in_memory: bool = False
    ):
        self.data_dir = data_dir
        self.ref_date = DB.normalize_date(ref_date)
        self.in_memory = in_memory
        if must_exist and in_memory:
            raise ValueError(
                'inconsistent options to DB: in_memory and must_exist'
            )
        exists = not self.in_memory and os.path.exists(self.db_file())
        if must_exist and not exists:
            raise FileNotFoundError(
                f'database file {self.db_file()} does not exist'
            )
        if not self.in_memory:
            os.makedirs(os.path.dirname(self.db_file()), exist_ok=True)
        self.created = self.in_memory or not exists
        self.conn = sqlite3.connect(self.db_file())

    def db_file(self):
        if self.in_memory:
            return ':memory:'
        else:
            return DB.db_path(self.data_dir, self.ref_date)

    @staticmethod
    def normalize_date(ref_date: datetime | date | int) -> datetime:
        """Convert a date to a datetime object."""
        if isinstance(ref_date, int):
            ref_date = datetime.fromtimestamp(ref_date)
        if isinstance(ref_date, date):
            ref_date = datetime(ref_date.year, ref_date.month, ref_date.day)
        return ref_date

    @staticmethod
    def db_path(data_dir: str, ref_date: datetime | date | int) -> str:
        ref_date = DB.normalize_date(ref_date)
        yr = ref_date.year
        doy = ref_date.timetuple().tm_yday
        return os.path.join(data_dir, f'{yr:04d}/{yr:04d}-{doy:03d}.sqlite')

    def cursor(self):
        assert self.conn is not None, 'DB connection is not open'
        return self.conn.cursor()

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

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
            callsign: str | None = None,
            orig: str | None = None,
            dest: str | None = None,
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

        def string_condition(field_name: str, value_or_pattern: str | None):
            if value_or_pattern is not None:
                if value_or_pattern.find('*') != -1:
                    id_conditions.append((
                        f'{field_name} LIKE ?', value_or_pattern.replace('*', '%')
                    ))
                else:
                    id_conditions.append((f'{field_name} = ?', value_or_pattern))

        string_condition('callsign', callsign)
        string_condition('orig', orig)
        string_condition('dest', dest)

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
