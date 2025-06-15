from abc import ABC, abstractmethod
import bz2
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import Enum, auto
import logging
from typing import Self

import pandas as pd

from feder_common.utils import Packer, Unpacker
import feder_common.models as models

from .models import Fix


logger = logging.getLogger(__name__)


class Message(ABC):
    MESSAGE_TAG: str = None  # type: ignore

    @staticmethod
    def unpack(data: bytes) -> type['Message']:
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
        assert self.MESSAGE_TAG is not None
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
                        time=datetime.fromtimestamp(f.time, tz=timezone.utc),
                        lon=f.lon, lat=f.lat,
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


@dataclass
class EndOfDay(Message):
    MESSAGE_TAG = 'd'

    day: date

    def _pack(self, packer: Packer):
        packer('>L', int(self.day.toordinal()))

    @classmethod
    def _unpack(cls, unpacker: Unpacker) -> Self:
        day = date.fromordinal(unpacker('>L'))
        return cls(day=day)


class Liveness(Enum):
    UNKNOWN = auto()
    OK = auto()
    ERROR = auto()


@dataclass
class IngesterLivenessQuery(Message):
    MESSAGE_TAG = 'L'

    source: str

    def _pack(self, packer: Packer):
        packer.str(self.source)

    @classmethod
    def _unpack(cls, unpacker: Unpacker) -> Self:
        return cls(source=unpacker.str())


@dataclass
class IngesterLivenessResponse(Message):
    MESSAGE_TAG = 'l'

    source: str
    time: datetime
    status: Liveness
    last_ingested: int = 0

    def _pack(self, packer: Packer):
        packer.str(self.source)
        packer('>Q', int(self.time.timestamp()))
        packer('>B', self.status.value)
        packer('>q', self.last_ingested)

    @classmethod
    def _unpack(cls, unpacker: Unpacker) -> Self:
        return cls(
            source=unpacker.str(),
            time=datetime.fromtimestamp(unpacker('>Q'), tz=timezone.utc),
            status=Liveness(unpacker('>B')),
            last_ingested=unpacker('>q')
        )


MESSAGE_TAG_DICT = {
    c.MESSAGE_TAG: c for c in [
        Trajectory, TrajectoryBatch, EndOfDay,
        IngesterLivenessQuery, IngesterLivenessResponse
    ]
}


def _single_value_check(
        source_id: str,
        fixes: list[Fix],
        field_name: str,
        choice_index: int | None = None
):
    vals = [getattr(f, field_name) for f in fixes]
    if len(set(vals)) != 1:
        msg = f'inconsistent {field_name} for {source_id} in position fixes: {vals}'

        # Choose a value from the non-null items.
        vals = [v for v in vals if v is not None]
        if len(vals) == 0:
            logger.warning(msg + ' (no suitable value found)')
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


def _substitute_none(x):
    return -999999 if x is None or pd.isna(x) else x
