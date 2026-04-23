"""Feder database access. Most of the contents of this module are for internal
use only, but the `QueryType` enumeration is needed for setting the spatial
overlap characteristics of queries."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, date, timezone
from enum import Enum, auto
from itertools import batched
import os
import sqlite3
from typing import Callable, Generator

import lz4.frame
import numpy as np

from .models import DataSource, Point, Trajectory
from .utils import MISSING_VALUE

_N_WORKERS = min(8, os.cpu_count() or 4)
_pool = ThreadPoolExecutor(max_workers=_N_WORKERS)

# Maximum number of trajectory IDs sent in a single `IN (?,?,...)` clause.
# Keeps the total bind-parameter count below SQLite's default
# SQLITE_MAX_VARIABLE_NUMBER (32766 on modern builds).
_ID_BATCH_SIZE = 5000


_BLOB_VERSION = 0x01


def _process_blob(
        blob: bytes,
        points_check: Callable[[np.ndarray], bool] | None,
        pt_filter: Callable[[np.ndarray], np.ndarray] | None,
) -> np.ndarray | None:
    version = blob[0]
    if version != _BLOB_VERSION:
        raise ValueError(
            f'unsupported blob version {version:#04x} — '
            'is the feder library up to date?'
        )
    arr = Point._unpack_blob(lz4.frame.decompress(blob[1:]))
    if points_check is not None and not points_check(arr):
        return None
    if pt_filter is not None:
        arr = pt_filter(arr)
    return arr


def _process_chunk(
        rows: list,
        points_check: Callable[[np.ndarray], bool] | None,
        pt_filter: Callable[[np.ndarray], np.ndarray] | None,
) -> list[tuple]:
    results = []
    for row in rows:
        arr = _process_blob(row[7], points_check, pt_filter)
        if arr is not None:
            results.append((row, Point._array_to_points(arr)))
    return results


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
    """Minimum altitude in feet."""
    max_alt: float | None = None
    """Maximum altitude in feet."""

    @property
    def has_bounds(self) -> bool:
        """Whether any bounds are set."""
        return any(
            b is not None for b in [
                self.min_lat, self.max_lat, self.min_lon, self.max_lon,
                self.min_alt, self.max_alt
            ]
        )

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
            in_memory: bool = False,
            read_only: bool = True
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
        if not read_only:
            self.conn = sqlite3.connect(self.db_file())
        else:
            self.conn = sqlite3.connect(
                f'file:{self.db_file()}?mode=ro', uri=True
            )

    def db_file(self):
        if self.in_memory:
            return ':memory:'
        else:
            return DB.db_path(self.data_dir, self.ref_date)

    @staticmethod
    def normalize_date(ref_date: datetime | date | int) -> datetime:
        """Convert a date to a datetime object."""
        if isinstance(ref_date, int):
            ref_date = datetime.fromtimestamp(ref_date, tz=timezone.utc)
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

    def get_flight_by_source_id(
            self, source: DataSource, source_id: str
    ) -> Trajectory | None:
        # Return a single result from _retrieve's generator.
        return next(self._retrieve([], source, source_ids=[source_id]), None)

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
            spatial_query_type: SpatialQueryType | None = None,
            filter_waypoints: bool = False
    ) -> Generator[Trajectory, None, None]:
        id_conditions = []
        pt_conditions = []

        if max_time < min_time:
            raise ValueError('flight query max_time must be larger than min_time')

        def time_condition(field1: str, cond1: str, field2: str, cond2: str):
            id_conditions.append((f'{field1}_timestamp {cond1} ?', min_time.timestamp()))
            id_conditions.append((f'{field2}_timestamp {cond2} ?', max_time.timestamp()))
            if filter_waypoints:
                pt_conditions.append(('time >= ?', min_time.timestamp()))
                pt_conditions.append(('time <= ?', max_time.timestamp()))

        match temporal_query_type:
            case TemporalQueryType.INTERSECTS:
                time_condition('max', '>=', 'min', '<=')
            case TemporalQueryType.WITHIN:
                time_condition('min', '>=', 'max', '<=')
            case TemporalQueryType.STARTS_IN:
                time_condition('min', '>=', 'min', '<=')
            case TemporalQueryType.ENDS_IN:
                time_condition('max', '>=', 'max', '<=')

        # Callable for post-bounding box filtering checking, used for spatially
        # crossing queries.
        points_check = None

        if bounds is None:
            bounds = BoundingBox()
        if bounds.has_bounds:
            if spatial_query_type is None:
                raise ValueError(
                    'spatial_query_type must be specified if bounds are given'
                )
            bounds._check(spatial_query_type)

            match spatial_query_type:
                case SpatialQueryType.CROSSES:
                    # ID conditions filter on bounding box overlap at the
                    # trajectory envelope level; points_check then confirms at
                    # least one actual point lies inside the query box.
                    if bounds.min_lat is not None:
                        id_conditions.append(('max_latitude >= ?', bounds.min_lat))
                        if filter_waypoints:
                            pt_conditions.append(('lat >= ?', bounds.min_lat))
                    if bounds.max_lat is not None:
                        id_conditions.append(('min_latitude <= ?', bounds.max_lat))
                        if filter_waypoints:
                            pt_conditions.append(('lat <= ?', bounds.max_lat))
                    if bounds.min_lon is not None:
                        id_conditions.append(('max_longitude >= ?', bounds.min_lon))
                        if filter_waypoints:
                            pt_conditions.append(('lon >= ?', bounds.min_lon))
                    if bounds.max_lon is not None:
                        id_conditions.append(('min_longitude <= ?', bounds.max_lon))
                        if filter_waypoints:
                            pt_conditions.append(('lon <= ?', bounds.max_lon))
                    if bounds.min_alt is not None:
                        id_conditions.append(('max_altitude >= ?', bounds.min_alt))
                        if filter_waypoints:
                            pt_conditions.append(('alt >= ?', bounds.min_alt))
                    if bounds.max_alt is not None:
                        id_conditions.append(('min_altitude <= ?', bounds.max_alt))
                        if filter_waypoints:
                            pt_conditions.append(('alt <= ?', bounds.max_alt))
                    _min_lat = bounds.min_lat
                    _max_lat = bounds.max_lat
                    _min_lon = bounds.min_lon
                    _max_lon = bounds.max_lon
                    _min_alt = bounds.min_alt
                    _max_alt = bounds.max_alt
                    def points_checker(arr: np.ndarray) -> bool:
                        mask = np.ones(len(arr), dtype=bool)
                        if _min_lat is not None:
                            mask &= arr['lat'] >= _min_lat
                        if _max_lat is not None:
                            mask &= arr['lat'] <= _max_lat
                        if _min_lon is not None:
                            mask &= arr['lon'] >= _min_lon
                        if _max_lon is not None:
                            mask &= arr['lon'] <= _max_lon
                        if _min_alt is not None:
                            mask &= (arr['alt'] != MISSING_VALUE) & (arr['alt'] >= _min_alt)
                        if _max_alt is not None:
                            mask &= (arr['alt'] != MISSING_VALUE) & (arr['alt'] <= _max_alt)
                        return bool(np.any(mask))
                    points_check = points_checker
                case SpatialQueryType.WITHIN:
                    if bounds.min_lat is not None and bounds.max_lat is not None:
                        id_conditions.append(('min_latitude >= ?', bounds.min_lat))
                        id_conditions.append(('max_latitude <= ?', bounds.max_lat))
                        if filter_waypoints:
                            pt_conditions.append(('lat >= ?', bounds.min_lat))
                            pt_conditions.append(('lat <= ?', bounds.max_lat))
                    if bounds.min_lon is not None and bounds.max_lon is not None:
                        id_conditions.append(('min_longitude >= ?', bounds.min_lon))
                        id_conditions.append(('max_longitude <= ?', bounds.max_lon))
                        if filter_waypoints:
                            pt_conditions.append(('lon >= ?', bounds.min_lon))
                            pt_conditions.append(('lon <= ?', bounds.max_lon))
                    if bounds.min_alt is not None and bounds.max_alt is not None:
                        id_conditions.append(('min_altitude >= ?', bounds.min_alt))
                        id_conditions.append(('max_altitude <= ?', bounds.max_alt))
                        if filter_waypoints:
                            pt_conditions.append(('alt >= ?', bounds.min_alt))
                            pt_conditions.append(('alt <= ?', bounds.max_alt))

        id_sql = (
            'SELECT id FROM trajectory_index WHERE ' +
            ' AND '.join(p[0] for p in id_conditions)
        )
        id_parameters = tuple(p[1] for p in id_conditions)

        cur = self.cursor()
        cur.execute(id_sql, id_parameters)
        ids = [t[0] for t in cur.fetchall()]

        for batch in batched(ids, _ID_BATCH_SIZE):
            yield from self._retrieve(
                pt_conditions, source, callsign, orig, dest,
                ids=list(batch), points_check=points_check
            )

    def _retrieve(
            self,
            pt_conditions: list[tuple[str, object]],
            source: DataSource | None,
            callsign: str | None = None,
            orig: str | None = None,
            dest: str | None = None,
            ids: str | list[str] | None = None,
            source_ids: str | list[str] | None = None,
            points_check: Callable[[np.ndarray], bool] | None = None,
    ) -> Generator[Trajectory, None, None]:
        # Normalize ID list parameters.
        if ids is None:
            ids = []
        elif not isinstance(ids, list):
            ids = [ids]
        if source_ids is None:
            source_ids = []
        elif not isinstance(source_ids, list):
            source_ids = [source_ids]

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

        partial = len(pt_conditions) > 0
        pt_filter = self._make_numpy_point_filter(pt_conditions) if partial else None

        # Build SQL and query parameters from parallel lists.
        traj_sql = (
            """SELECT source, source_id, transponder_id, orig, dest,
                      callsign, aircraft_type, points
                 FROM trajectories WHERE """ +
                 self._build_sql_conditions(conditions, ids, source_ids)
        )
        traj_cur = self.cursor()
        traj_cur.execute(
            traj_sql,
            tuple(p[1] for p in conditions) + tuple(ids) + tuple(source_ids)
        )

        rows = list(traj_cur)
        if not rows:
            return

        # Split into at most N_WORKERS chunks so thread-pool overhead is O(workers)
        # rather than O(trajectories).  _array_to_points runs inside the threads too.
        n_chunks = min(_N_WORKERS, len(rows))
        chunk_size = -(-len(rows) // n_chunks)  # ceiling division
        chunks = [rows[i:i + chunk_size] for i in range(0, len(rows), chunk_size)]

        futures = [
            _pool.submit(_process_chunk, chunk, points_check, pt_filter)
            for chunk in chunks
        ]
        for future in futures:
            for traj_rec, points in future.result():
                yield Trajectory(
                    source=DataSource(traj_rec[0]), source_id=traj_rec[1],
                    transponder_id=traj_rec[2], orig=traj_rec[3], dest=traj_rec[4],
                    callsign=traj_rec[5], aircraft_type=traj_rec[6],
                    points=points, partial=partial
                )

    def _build_sql_conditions(self, conditions, ids, source_ids):
        sql_conditions = [p[0] for p in conditions]
        if len(ids) > 0:
            sql_conditions.append(
                'id IN (' + ','.join('?' for _ in ids) + ')'
            )
        if len(source_ids) > 0:
            sql_conditions.append(
                'source_id IN (' + ','.join('?' for _ in source_ids) + ')'
            )
        return ' AND '.join(sql_conditions)

    def _build_point_sql_conditions(self, conditions):
        if len(conditions) == 0:
            return ''
        return ' AND ' + ' AND '.join(p[0] for p in conditions)

    def _make_numpy_point_filter(
            self, pt_conditions: list[tuple[str, object]]
    ) -> Callable[[np.ndarray], np.ndarray]:
        def point_filter(arr: np.ndarray) -> np.ndarray:
            if len(arr) == 0:
                return arr
            mask = np.ones(len(arr), dtype=bool)
            for cond, val in pt_conditions:
                match cond:
                    case 'time >= ?':
                        mask &= arr['time'] >= val
                    case 'time <= ?':
                        mask &= arr['time'] <= val
                    case 'lat >= ?':
                        mask &= arr['lat'] >= val
                    case 'lat <= ?':
                        mask &= arr['lat'] <= val
                    case 'lon >= ?':
                        mask &= arr['lon'] >= val
                    case 'lon <= ?':
                        mask &= arr['lon'] <= val
                    case 'alt >= ?':
                        mask &= (arr['alt'] != MISSING_VALUE) & (arr['alt'] >= val)
                    case 'alt <= ?':
                        mask &= (arr['alt'] != MISSING_VALUE) & (arr['alt'] <= val)
                    case _:
                        raise ValueError(f'unsupported point condition {cond}')
            return arr[mask]
        return point_filter
