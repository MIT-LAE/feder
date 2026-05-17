"""Common data models used throughout the Feder system."""

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from operator import attrgetter
from typing import Self, cast

import numpy as np

from .utils import Packer, Unpacker, encode_opt_float, decode_opt_float

# Numpy structured dtype mirroring Point._POINT_FORMAT ('>Lddddd?').
# Used by Point._unpack_blob and Point.unpack to parse a full decompressed
# point blob in a single np.frombuffer call instead of a per-point Python loop.
_POINT_DTYPE = np.dtype([
    ('time',     '>u4'),
    ('lon',      '>f8'),
    ('lat',      '>f8'),
    ('alt',      '>f8'),
    ('alt_gnss', '>f8'),
    ('heading',  '>f8'),
    ('on_ground', '?'),
])


class DataSource(Enum):
    """Flight data sources known to Feder."""
    FLIGHTAWARE = 1
    """Historical CSV files from the FAST system."""
    CONTRAILS_API = 2
    """Contrails API Spire ADS-B source."""
    OPENSKY = 3
    """OpenSky simplified trajectories source. *[NOT YET IMPLEMENTED]*"""
    OPENSKY_STATE_VECTORS = 4
    """OpenSky state vectors source. *[NOT YET IMPLEMENTED]*"""

    def __str__(self):
        return self.name.lower().replace('_', '-')


@dataclass(slots=True)
class Point:
    """A single point in a trajectory."""

    _POINT_FORMAT = '>Lddddd?'

    time: datetime
    """The time of the point as a Python UTC datetime."""
    lon: float
    """Longitude in decimal degrees."""
    lat: float
    """Latitude in decimal degrees."""
    alt: float | None
    """Altitude in feet, or None if not available."""
    alt_gnss: float | None
    """GNSS altitude in feet, or None if not available."""
    heading: float | None
    """Heading in degrees, or None if not available."""
    on_ground: bool
    """True if the aircraft is on the ground, False otherwise."""

    @classmethod
    def pack(cls, points: list[Self], packer: Packer | None = None) -> bytes:
        """Pack to binary data. @private"""
        if packer is None:
            packer = Packer()
        packer('>H', len(points))
        for p in points:
            packer(
                cls._POINT_FORMAT,
                int(p.time.timestamp()),
                p.lon, p.lat,
                encode_opt_float(p.alt),
                encode_opt_float(p.alt_gnss),
                encode_opt_float(p.heading),
                p.on_ground
            )
        return packer.data()

    @classmethod
    def unpack(
            cls,
            data: bytes | None,
            unpacker: Unpacker | None = None
    ) -> list[Self]:
        """Unpack from binary data. @private"""
        if data is None and unpacker is None:
            raise ValueError('must provide data or unpacker to Point.unpack')
        if unpacker is None:
            assert data is not None
            unpacker = Unpacker(data)
        npoints = cast(int, unpacker('>H'))
        if npoints == 0:
            return []
        raw = unpacker._buf.read(npoints * _POINT_DTYPE.itemsize)
        return cls._array_to_points(np.frombuffer(raw, dtype=_POINT_DTYPE, count=npoints))

    @classmethod
    def _unpack_blob(cls, data: bytes) -> np.ndarray:
        """Parse a decompressed point blob to a numpy array. @private"""
        npoints = int.from_bytes(data[:2], byteorder='big')
        if npoints == 0:
            return np.zeros(0, dtype=_POINT_DTYPE)
        return np.frombuffer(data, dtype=_POINT_DTYPE, offset=2, count=npoints)

    @classmethod
    def _array_to_points(cls, arr: np.ndarray) -> list[Self]:
        """Convert a numpy point array to a list of Point objects. @private"""
        return [
            cls(
                time=datetime.fromtimestamp(int(row['time']), tz=timezone.utc),
                lon=float(row['lon']),
                lat=float(row['lat']),
                alt=decode_opt_float(float(row['alt'])),
                alt_gnss=decode_opt_float(float(row['alt_gnss'])),
                heading=decode_opt_float(float(row['heading'])),
                on_ground=bool(row['on_ground'])
            )
            for row in arr
        ]


@dataclass(slots=True)
class TrajectoryArray:
    """A flight trajectory with points represented as a numpy array.

    This is a lower-level representation intended for bulk processing. The
    point array fields are `time`, `lon`, `lat`, `alt`, `alt_gnss`, `heading`
    and `on_ground`. Callers should treat point arrays as read-only and copy
    them before mutating.
    """

    source_id: str
    """The unique source-specific ID of the trajectory."""
    source: DataSource
    """The data source for the trajectory."""
    transponder_id: str
    """The ADS-B transponder ID of the aircraft."""
    orig: str | None
    """The ICAO origin airport code."""
    dest: str | None
    """The ICAO destination airport code."""
    callsign: str
    """The callsign of the aircraft."""
    aircraft_type: str | None
    """The ICAO type of the aircraft."""
    points: np.ndarray
    """Structured numpy array of trajectory points."""
    partial: bool = False
    """Was the trajectory generated from a query using waypoint filtering?"""


@dataclass(slots=True)
class Trajectory:
    """A single flight trajectory."""

    source_id: str
    """The unique source-specific ID of the trajectory."""
    source: DataSource
    """The data source for the trajectory."""
    transponder_id: str
    """The ADS-B transponder ID of the aircraft."""
    orig: str | None
    """The ICAO origin airport code."""
    dest: str | None
    """The ICAO destination airport code."""
    callsign: str
    """The callsign of the aircraft."""
    aircraft_type: str | None
    """The ICAO type of the aircraft."""
    points: list[Point]
    """A list of points in the trajectory."""
    partial: bool = False
    """Was the trajectory generated from a query using waypoint filtering?"""

    def pack(self, packer: Packer | None = None) -> bytes:
        """Pack to binary data. @private"""
        if packer is None:
            packer = Packer()
        packer('>B', self.source.value)
        packer.str(self.source_id)
        packer.str(self.transponder_id)
        packer.str(self.orig)
        packer.str(self.dest)
        packer.str(self.callsign)
        packer.str(self.aircraft_type)
        Point.pack(self.points, packer=packer)
        return packer.data()

    @classmethod
    def unpack(
            cls,
            data: bytes | None,
            unpacker: Unpacker | None = None
    ) -> Self:
        """Unpack from binary data. @private"""
        if data is None and unpacker is None:
            raise ValueError('must provide data or unpacker to Trajectory.unpack')
        if unpacker is None:
            assert data is not None
            unpacker = Unpacker(data)
        return cls(
            source=DataSource(unpacker('>B')),
            source_id=cast(str, unpacker.str()),
            transponder_id=cast(str, unpacker.str()),
            orig=unpacker.str(),
            dest=unpacker.str(),
            callsign=cast(str, unpacker.str()),
            aircraft_type=unpacker.str(),
            points=Point.unpack(data=None, unpacker=unpacker)
        )

    def merge(self, *others: Self | None) -> Self:
        """Merge trajectories. @private

        Trajectory points are combined in time order, and metadata values are
        combined using majority voting between the trajectories."""
        points = {}
        for o in others:
            if o is None:
                continue
            for p in o.points:
                points[p.time] = p
        for p in self.points:
            points[p.time] = p
        weights = [len(self.points)] + [len(o.points) if o is not None else 0 for o in others]
        return type(self)(
            source=self.source,
            source_id=self.source_id,
            transponder_id = _majority(weights, 'transponder_id', self, *others) or '',
            orig = _majority(weights, 'orig', self, *others),
            dest = _majority(weights, 'dest', self, *others),
            callsign = _majority(weights, 'callsign', self, *others) or '',
            aircraft_type = _majority(weights, 'aircraft_type', self, *others),
            points = sorted(points.values(), key=attrgetter('time'))
        )


def _majority(
        weights: list[int],
        attr: str, *trajs: Trajectory | None
) -> str | None:
    values = [getattr(t, attr) if t is not None else None for t in trajs]
    counts = Counter()
    for w, v in zip(weights, values):
        if v is None:
            continue
        counts[v] += w
    return counts.most_common(1)[0][0] if len(counts) > 0 else None
