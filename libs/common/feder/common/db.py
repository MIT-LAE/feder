from dataclasses import dataclass
from operator import attrgetter

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
