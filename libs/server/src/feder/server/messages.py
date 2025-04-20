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
    MESSAGE_TAG = 'T'

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
            cls, source: models.DataSource, source_id: str, df: pd.DataFrame
    ) -> Self:
        try:
            _single_value_column_check(df, 'transponder_id')
            _single_value_column_check(df, 'callsign')
            _single_value_column_check(df, 'aircraft_type')
            return cls(
                model=models.Trajectory(
                    source=source,
                    id=source_id,
                    transponder_id=df.transponder_id[0] or '',
                    callsign=df.callsign[0] or '',
                    aircraft_type = df.aircraft_type[0] or '',
                    points=[
                        models.Point(
                            time=datetime.fromtimestamp(t.time), lon=t.lon, lat=t.lat,
                            alt=_substitute_none(t.alt),
                            alt_gnss=_substitute_none(t.alt_gnss),
                            heading=_substitute_none(t.heading),
                            on_ground=t.on_ground
                        ) for t in df.itertuples()
                    ]
                )
            )
        except Exception as e:
            # TODO: Fix this
            print('OOPS')
            print(e)
            print(df)


class Liveness(Enum):
    UNKNOWN = auto()
    OK = auto()
    ERROR = auto()


@dataclass
class LivenessQuery(Message):
    MESSAGE_TAG = 'L'

    source: str

    def _pack(self, packer: Packer):
        packer.str(self.source)

    @classmethod
    def _unpack(cls, unpacker: Unpacker) -> Self:
        return cls(source=unpacker.str())


@dataclass
class LivenessResponse(Message):
    MESSAGE_TAG = 'l'

    source: str
    time: datetime
    status: Liveness

    def _pack(self, packer: Packer):
        packer.str(self.source)
        packer('>Q', int(self.time.timestamp()))
        packer('>B', self.status.value)

    @classmethod
    def _unpack(cls, unpacker: Unpacker) -> Self:
        return cls(
            source=unpacker.str(),
            time=datetime.fromtimestamp(unpacker('>Q')),
            status=Liveness(unpacker('>B'))
        )


MESSAGE_TAG_DICT = {
    c.MESSAGE_TAG: c for c in [
        Trajectory, LivenessQuery, LivenessResponse
    ]
}


def _single_value_column_check(df: pd.DataFrame, column_name: str):
    if len(set(df[column_name])) != 1:
        msg = f'inconsistent {column_name} in position fixes: {list(df[column_name])}'
        logger.warn(msg)

        # Just take a majority vote.
        df[column_name] = Counter(df[column_name]).most_common(1)[0][0]


def _substitute_none(xs):
    return np.where(pd.isna(xs), -999999, xs)
