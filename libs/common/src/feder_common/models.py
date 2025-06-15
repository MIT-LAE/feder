"""Common data models used throughout the Feder system."""

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from operator import attrgetter
from typing import Self

from .utils import (
    Packer, Unpacker, encode_opt_float, decode_opt_float, milli
)


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
    """Altitude in meters, or None if not available."""
    alt_gnss: float | None
    """GNSS altitude in meters, or None if not available."""
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
            unpacker = Unpacker(data)
        npoints = unpacker('>H')
        points = []
        for i in range(npoints):
            pt = unpacker(cls._POINT_FORMAT, multiple=True)
            points.append(cls(
                time=datetime.fromtimestamp(pt[0], tz=timezone.utc),
                lon=milli(pt[1]), lat=milli(pt[2]),
                alt=decode_opt_float(milli(pt[3])),
                alt_gnss=decode_opt_float(milli(pt[4])),
                heading=decode_opt_float(milli(pt[5])),
                on_ground=pt[6]
            ))
        return points


@dataclass(slots=True)
class Trajectory:
    """A single flight trajectory."""

    source_id: str
    """The data source for the trajectory."""
    source: DataSource
    """The unique source-specific ID of the trajectory."""
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
            unpacker = Unpacker(data)
        return cls(
            source=DataSource(unpacker('>B')),
            source_id=unpacker.str(),
            transponder_id=unpacker.str(),
            orig=unpacker.str(),
            dest=unpacker.str(),
            callsign=unpacker.str(),
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
