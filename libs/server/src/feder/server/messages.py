from abc import ABC, abstractmethod
import bz2
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from enum import Enum, auto
import logging
from typing import Self

import numpy as np
import pandas as pd

from feder.common.utils import Packer, Unpacker
import feder.common.models as models

from .models import Fix


logger = logging.getLogger(__name__)


@dataclass
class Message(ABC):
    MESSAGE_TAG = None

    @staticmethod
    def unpack(data: bytes) -> Self:
        tag = chr(data[0])
        message_class = MESSAGE_TAG_DICT.get(tag)
        if message_class is None:
            raise ValueError(
                f'unknown message tag "{tag}" for message'
            )
        return message_class._unpack(Unpacker(bz2.decompress(data[1:])))

    def pack(self) -> bytes:
        packer = Packer()
        self._pack(packer)
        return bytes([ord(self.MESSAGE_TAG)]) + bz2.compress(packer.data())

    @abstractmethod
    def _pack(self, packer: Packer):
        ...

    @classmethod
    @abstractmethod
    def _unpack(cls, unpacker: Unpacker) -> Self:
        ...

@dataclass
class Trajectory(Message):
    MESSAGE_TAG = 't'

    model: models.Trajectory

    def _pack(self, packer: Packer):
       self.model.pack(packer=packer)

    @classmethod
    def _unpack(cls, unpacker: Unpacker) -> Self:
        return cls(
            model=models.Trajectory.unpack(
                data=None, unpacker=unpacker
            )
        )

    @classmethod
    def build(
            cls, source: models.DataSource, source_id: str, fixes: list[Fix]
    ) -> Self:
        # When fixing these, we take the latest non-null destination (assumed
        # to be the final destination after any flight plan changes), the
        # earliest non-null origin, and a majority vote for any others.
        _single_value_check(source_id, fixes, 'transponder_id')
        _single_value_check(source_id, fixes, 'callsign')
        _single_value_check(source_id, fixes, 'orig', choice_index=0)
        _single_value_check(source_id, fixes, 'dest', choice_index=-1)
        _single_value_check(source_id, fixes, 'aircraft_type')
        return cls(
            model=models.Trajectory(
                source=source,
                source_id=source_id,
                transponder_id=fixes[0].transponder_id or '',
                orig=fixes[0].orig or '',
                dest=fixes[0].dest or '',
                callsign=fixes[0].callsign or '',
                aircraft_type = fixes[0].aircraft_type or '',
                points=[
                    models.Point(
                        time=datetime.fromtimestamp(f.time), lon=f.lon, lat=f.lat,
                        alt=_substitute_none(f.alt),
                        alt_gnss=_substitute_none(f.alt_gnss),
                        heading=_substitute_none(f.heading),
                        on_ground=f.on_ground
                    ) for f in fixes
                ]
            )
        )


@dataclass
class TrajectoryBatch(Message):
    MESSAGE_TAG = 'T'

    trajectories: list[Trajectory]
    source: str
    trajectory_count: int

    def _pack(self, packer: Packer):
        packer('>B', len(self.trajectories))
        for traj in self.trajectories:
            traj.model.pack(packer=packer)
        packer.str(self.source)
        packer('>L', self.trajectory_count)

    @classmethod
    def _unpack(cls, unpacker: Unpacker) -> Self:
        ntrajs = int(unpacker('>B'))
        return cls(
            trajectories=[
                Trajectory(
                    model=models.Trajectory.unpack(
                        data=None, unpacker=unpacker
                    )
                )
                for i in range(ntrajs)
            ],
            source=unpacker.str(),
            trajectory_count=unpacker('>L')
        )


class Liveness(Enum):
    UNKNOWN = auto()
    OK = auto()
    ERROR = auto()


@dataclass
class LivenessQuery(Message):
    MESSAGE_TAG = 'L'

    source: str
    include_info: bool

    def _pack(self, packer: Packer):
        packer.str(self.source)
        packer('>?', self.include_info)

    @classmethod
    def _unpack(cls, unpacker: Unpacker) -> Self:
        return cls(
            source=unpacker.str(),
            include_info=unpacker('>?')
        )


@dataclass
class LivenessResponse(Message):
    MESSAGE_TAG = 'l'
    Info = dict[str, int | float | str | datetime] | None

    source: str
    time: datetime
    status: Liveness
    info: Info = None

    def _pack(self, packer: Packer):
        packer.str(self.source)
        packer('>Q', int(self.time.timestamp()))
        packer('>B', self.status.value)
        self._pack_info(packer)

    @classmethod
    def _unpack(cls, unpacker: Unpacker) -> Self:
        return cls(
            source=unpacker.str(),
            time=datetime.fromtimestamp(unpacker('>Q')),
            status=Liveness(unpacker('>B')),
            info=cls._unpack_info(unpacker)
        )

    def _pack_info(self, packer: Packer):
        if self.info is None:
            packer('>B', 0)
        else:
            packer('>B', len(self.info))
            for k, v in self.info.items():
                packer.str(k)
                if isinstance(v, int):
                    packer('>B', ord('I'))
                    packer('>q', v)
                elif isinstance(v, float):
                    packer('>B', ord('F'))
                    packer('>d', v)
                elif isinstance(v, str):
                    packer('>B', ord('S'))
                    packer.str(v)
                elif isinstance(v, datetime):
                    packer('>B', ord('D'))
                    packer('>L', int(v.timestamp()))
                else:
                    raise ValueError('invalid info property in LivenessResponse')

    @classmethod
    def _unpack_info(cls, unpacker: Unpacker):
        nentries = unpacker('>B')
        if nentries == 0:
            return None
        info = {}
        for i in range(nentries):
            k = unpacker.str()
            tag = chr(unpacker('>B'))
            match tag:
                case 'I':
                    v = unpacker('>q')
                case 'F':
                    v = unpacker('>d')
                case 'S':
                    v = unpacker.str()
                case 'D':
                    v = datetime.fromtimestamp(unpacker('>L'))
                case _:
                    raise ValueError('invalid info property in LivenessResponse')
            info[k] = v
        return info


MESSAGE_TAG_DICT = {
    c.MESSAGE_TAG: c for c in [
        Trajectory, TrajectoryBatch, LivenessQuery, LivenessResponse
    ]
}


def _single_value_check(
        source_id: str,
        fixes: list[Fix],
        field_name: str,
        choice_index: int = None
):
    vals = [getattr(f, field_name) for f in fixes]
    if len(set(vals)) != 1:
        msg = f'inconsistent {field_name} for {source_id} in position fixes: {vals}'

        # Choose a value from the non-null items.
        vals = [v for v in vals if v is not None]
        if len(vals) == 0:
            logger.warn(msg + ' (no suitable value found)')
            return

        # If an index is specified, use it. Otherwise take a majority vote.
        selected = None
        if choice_index is not None:
            selected = vals[choice_index]
        else:
            selected = Counter(vals).most_common(1)[0][0]
        logger.debug(msg + f' (selected "{selected}")')
        for f in fixes:
            setattr(f, field_name, selected)


def _substitute_none(xs):
    return np.where(pd.isna(xs), -999999, xs)
