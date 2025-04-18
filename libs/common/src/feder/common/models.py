from dataclasses import dataclass
from datetime import datetime
import struct
from typing import Self

from .utils import (
    Packer, Unpacker, encode_opt_float, decode_opt_float, milli
)


@dataclass
class Point:
    POINT_FORMAT = '>Lddddd?'

    time: datetime
    lon: float
    lat: float
    alt: float
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
                p[1].lon, p[1].lat, p[1].alt,
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
    id: str
    source: str
    transponder_id: str
    callsign: str
    aircraft_type: str | None
    points: list[Point]

    def pack(self, packer: Packer | None = None) -> bytes:
        if packer is None:
            packer = Packer()
        packer.str(self.source)
        packer.str(self.id)
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
            source=unpacker.str(),
            id=unpacker.str(),
            transpondeR_id=unpacker.str(),
            callsign=unpacker.str(),
            aircraft_type=unpacker.str(),
            points=Point.unpack(data=None, unpacker=unpacker)
        )
