from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from operator import attrgetter
from typing import Self

from .utils import (
    Packer, Unpacker, encode_opt_float, decode_opt_float, milli
)


class DataSource(Enum):
    FLIGHTAWARE = 1
    CONTRAILS_API = 2
    OPENSKY = 3
    OPENSKY_STATE_VECTORS = 4

    def __str__(self):
        return self.name.lower().replace('_', '-')


@dataclass
class Point:
    POINT_FORMAT = '>Lddddd?'

    time: datetime
    lon: float
    lat: float
    alt: float | None
    alt_gnss: float | None
    heading: float | None
    on_ground: bool

    @classmethod
    def pack(cls, points: list[Self], packer: Packer | None = None) -> bytes:
        if packer is None:
            packer = Packer()
        for p in enumerate(points):
            packer(
                cls.POINT_FORMAT,
                int(p[1].time.timestamp()),
                p[1].lon, p[1].lat,
                encode_opt_float(p[1].alt),
                encode_opt_float(p[1].alt_gnss),
                encode_opt_float(p[1].heading),
                p[1].on_ground
            )
        return packer.data()

    @classmethod
    def unpack(
            cls,
            data: bytes | None,
            unpacker: Unpacker | None = None
    ) -> list[Self]:
        if data is None and unpacker is None:
            raise ValueError('must provide data or unpacker to Point.unpack')
        if unpacker is None:
            unpacker = Unpacker(data)
        return [
            cls(
                time=datetime.fromtimestamp(pt[0]),
                lon=milli(pt[1]), lat=milli(pt[2]),
                alt=decode_opt_float(milli(pt[3])),
                alt_gnss=decode_opt_float(milli(pt[4])),
                heading=decode_opt_float(milli(pt[5])),
                on_ground=pt[6]
            )
            for pt in unpacker.iter(cls.POINT_FORMAT, multiple=True)
        ]


@dataclass
class Trajectory:
    source_id: str
    source: DataSource
    transponder_id: str
    callsign: str
    aircraft_type: str | None
    points: list[Point]

    def pack(self, packer: Packer | None = None) -> bytes:
        if packer is None:
            packer = Packer()
        packer('>B', self.source.value)
        packer.str(self.source_id)
        packer.str(self.transponder_id)
        packer.str(self.callsign)
        packer.str(self.aircraft_type)
        Point.pack(self.points, packer=packer)

    @classmethod
    def unpack(
            cls,
            data: bytes | None,
            unpacker: Unpacker | None = None
    ) -> Self:
        if data is None and unpacker is None:
            raise ValueError('must provide data or unpacker to Trajectory.unpack')
        if unpacker is None:
            unpacker = Unpacker(data)
        return cls(
            source=DataSource(unpacker('>B')),
            source_id=unpacker.str(),
            transponder_id=unpacker.str(),
            callsign=unpacker.str(),
            aircraft_type=unpacker.str(),
            points=Point.unpack(data=None, unpacker=unpacker)
        )

    def merge(self, *others: Self | None) -> Self:
        points = {}
        for o in others:
            if o is None:
                continue
            for p in o.points:
                points[p.time] = p
        for p in self.points:
            points[p.time] = p
        weights = [len(self.points)] + [len(o.points) if o is not None else 0 for o in others]
        return Trajectory(
            source=self.source,
            source_id=self.source_id,
            transponder_id = _majority(weights, 'transponder_id', self, *others),
            callsign = _majority(weights, 'callsign', self, *others),
            aircraft_type = _majority(weights, 'aircraft_type', self, *others),
            points = sorted(points.values(), key=attrgetter('time'))
        )


def _majority(
        weights: list[int],
        attr: str, *trajs:
        list[Trajectory | None]
) -> str | None:
    values = [getattr(t, attr) if t is not None else None for t in trajs]
    counts = Counter()
    for w, v in zip(weights, values):
        if v is None:
            continue
        counts[v] += w
    return counts.most_common(1)[0][0] if len(counts) > 0 else None
