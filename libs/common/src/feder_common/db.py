"""Feder database access. Most of the contents of this module are for internal
use only, but the `QueryType` enumeration is needed for setting the spatial
overlap characteristics of queries."""

import bz2
from dataclasses import dataclass
from datetime import datetime, date
from enum import Enum, auto
from itertools import batched
import os
import sqlite3
from typing import Generator

from .models import DataSource, Point, Trajectory


class TemporalQueryType(Enum):
    """Temporal query types."""
    INTERSECTS = auto()
    """Temporal extent intersects a given time range."""
    WITHIN = auto()
    """Temporal extent is fully contained within a given time range."""
    STARTS_IN = auto()
    """Temporal extent starts within a given time range."""
    ENDS_IN = auto()
    """Temporal extent ends within a given time range."""


class SpatialQueryType(Enum):
    """Spatial query types."""
    CROSSES = auto()
    """Find trajectories that cross a bounding box."""
    WITHIN = auto()
    """Find trajectories contained within a bounding box."""


# TODO: Specify altitude units.

@dataclass(slots=True)
class BoundingBox:
    """Bounding box for spatial queries."""
    min_lat: float | None = None
    """Minimum latitude."""
    max_lat: float | None = None
    """Maximum latitude."""
    min_lon: float | None = None
    """Minimum longitude."""
    max_lon: float | None = None
    """Maximum longitude."""
    min_alt: float | None = None
    """Minimum altitude."""
    max_alt: float | None = None
    """Maximum altitude."""

    def _check(self, query_type: SpatialQueryType):
        if self.min_lat is not None and self.max_lat is not None:
            if self.min_lat > self.max_lat:
                raise ValueError('min_lat must be less than or equal to max_lat')
        if self.min_lon is not None and self.max_lon is not None:
            if self.min_lon > self.max_lon:
                raise ValueError('min_lon must be less than or equal to max_lon')
        if self.min_alt is not None and self.max_alt is not None:
            if self.min_alt > self.max_alt:
                raise ValueError('min_alt must be less than or equal to max_alt')
        if query_type == SpatialQueryType.WITHIN:
            if (self.min_lat is not None) != (self.max_lat is not None):
                raise ValueError(
                    'both or no latitude bounds needed for a within spatial query'
                )
            if (self.min_lon is not None) != (self.max_lon is not None):
                raise ValueError(
                    'both or no longitude bounds needed for a within spatial query'
                )
            if (self.min_alt is not None) != (self.max_alt is not None):
                raise ValueError(
                    'both or no altitude bounds needed for a within spatial query'
                )


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

    def format_date(self) -> str:
        yr = self.ref_date.year
        doy = self.ref_date.timetuple().tm_yday
        return f'{yr:04d}-{doy:03d}'

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

    def size(self) -> int:
        cur = self.cursor()
        cur.execute('SELECT COUNT(*) FROM trajectories')
        return cur.fetchone()[0]

    def get_flight_by_id(
            self, source: DataSource, source_id: str
    ) -> Trajectory | None:
        # Return a single result from _retrieve's generator.
        return next(self._retrieve(source, 'source_id = ?', source_id), None)

    def timestamp_ranges(self) -> list[tuple[int, int]]:
        cur = self.cursor()
        cur.execute('SELECT min_timestamp, max_timestamp FROM trajectory_index')
        return [(int(t[0]), int(t[1])) for t in cur.fetchall()]

    def data_sources(self) -> set[DataSource]:
        cur = self.cursor()
        cur.execute('SELECT DISTINCT source FROM trajectories')
        return set(DataSource(t[0]) for t in cur.fetchall())

    def query_flights(
            self,
            min_time: datetime,
            max_time: datetime,
            bounds: BoundingBox | None = None,
            source: DataSource | None = None,
            callsign: str | None = None,
            orig: str | None = None,
            dest: str | None = None,
            temporal_query_type: TemporalQueryType = TemporalQueryType.INTERSECTS,
            spatial_query_type: SpatialQueryType | None = None
    ) -> Generator[Trajectory, None, None]:
        id_conditions = []

        if max_time < min_time:
            raise ValueError('flight query max_time must be larger than min_time')

        def time_condition(field1: str, cond1: str, field2: str, cond2: str):
            id_conditions.append((f'{field1}_timestamp {cond1} ?', min_time.timestamp()))
            id_conditions.append((f'{field2}_timestamp {cond2} ?', max_time.timestamp()))

        match temporal_query_type:
            case TemporalQueryType.INTERSECTS:
                time_condition('max', '>=', 'min', '<=')
            case TemporalQueryType.WITHIN:
                time_condition('min', '>=', 'max', '<=')
            case TemporalQueryType.STARTS_IN:
                time_condition('min', '>=', 'min', '<=')
            case TemporalQueryType.ENDS_IN:
                time_condition('max', '>=', 'max', '<=')

        if bounds is not None:
            if spatial_query_type is None:
                raise ValueError(
                    'spatial_query_type must be specified if bounds are given'
                )
            bounds._check(spatial_query_type)

            match spatial_query_type:
                case SpatialQueryType.CROSSES:
                    if bounds.min_lat is not None:
                        id_conditions.append(('max_latitude >= ?', bounds.min_lat))
                    if bounds.max_lat is not None:
                        id_conditions.append(('min_latitude <= ?', bounds.max_lat))
                    if bounds.min_lon is not None:
                        id_conditions.append(('max_longitude >= ?', bounds.min_lon))
                    if bounds.max_lon is not None:
                        id_conditions.append(('min_longitude <= ?', bounds.max_lon))
                    if bounds.min_alt is not None:
                        id_conditions.append(('max_altitude >= ?', bounds.min_alt))
                    if bounds.max_alt is not None:
                        id_conditions.append(('min_altitude <= ?', bounds.max_alt))
                case SpatialQueryType.WITHIN:
                    if bounds.min_lat is not None and bounds.max_lat is not None:
                        id_conditions.append(('min_latitude >= ?', bounds.min_lat))
                        id_conditions.append(('max_latitude <= ?', bounds.max_lat))
                    if bounds.min_lon is not None and bounds.max_lon is not None:
                        id_conditions.append(('min_longitude >= ?', bounds.min_lon))
                        id_conditions.append(('max_longitude <= ?', bounds.max_lon))
                    if bounds.min_alt is not None and bounds.max_alt is not None:
                        id_conditions.append(('min_altitude >= ?', bounds.min_alt))
                        id_conditions.append(('max_altitude <= ?', bounds.max_alt))

        if source is not None:
            id_conditions.append(('source = ?', source.value))

        id_sql = (
            'SELECT id FROM trajectory_index WHERE ' +
            ' AND '.join(p[0] for p in id_conditions)
        )
        id_parameters = tuple(p[1] for p in id_conditions)

        cur = self.cursor()
        cur.execute(id_sql, id_parameters)
        ids = [t[0] for t in cur.fetchall()]

        # Batch the IDs to keep the length of SQL queries reasonable.
        for batch in batched(ids, 50):
            yield from self._retrieve(source, callsign, orig, dest, list(batch))

    def _retrieve(
            self,
            source: DataSource | None,
            callsign: str | None,
            orig: str | None,
            dest: str | None,
            ids: str | list[str]
    ) -> Generator[Trajectory, None, None]:
        # Normalize ID list parameter.
        if not isinstance(ids, list):
            ids = [ids]

        conditions = []

        if source is not None:
            conditions.append(('source = ?', source.value))

        def string_condition(field_name: str, value_or_pattern: str | None):
            if value_or_pattern is not None:
                if value_or_pattern.find('*') != -1:
                    conditions.append((
                        f'{field_name} LIKE ?', value_or_pattern.replace('*', '%')
                    ))
                else:
                    conditions.append((f'{field_name} = ?', value_or_pattern))

        string_condition('callsign', callsign)
        string_condition('orig', orig)
        string_condition('dest', dest)

        # Build SQL and query parameters from parallel lists.
        sql = (
            """SELECT source, source_id, transponder_id, orig, dest,
                      callsign, aircraft_type, points
                 FROM trajectories WHERE """ +
            ' AND '.join(p[0] for p in conditions) +
            f'AND id IN ({",".join("?" for _ in ids)})'
        )
        parameters = tuple(p[1] for p in conditions) + tuple(ids)

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
