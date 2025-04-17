from dataclasses import dataclass
from operator import attrgetter
import os
import sqlite3

from .points_pb2 import Points


@dataclass
class PositionFix:
    time: int
    lat: float
    lon: float
    alt: int


@dataclass
class Trajectory:
    id: str
    callsign: str
    points: list[PositionFix]

    def _range(self, attr):
        min_pt = min(self.points, key=attrgetter(attr))
        max_pt = max(self.points, key=attrgetter(attr))
        return getattr(min_pt, attr), getattr(max_pt, attr)

    def time_range(self):
        return self._range('time')

    def lat_range(self):
        return self._range('lat')

    def lon_range(self):
        return self._range('lon')

    def alt_range(self):
        return self._range('alt')


class DB:
    def __init__(self, db_file: str):
        self.db_file = db_file
        self._conn = None

    @property
    def conn(self):
        if not os.path.exists(self.db_file):
            raise ValueError(f'database file {self.db_file} does not exist')
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_file)
        return self._conn

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None
