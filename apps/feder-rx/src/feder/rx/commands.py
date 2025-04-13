from dataclasses import dataclass
from datetime import datetime
from functools import total_ordering

import feder.server.rmq as rmq


# Classes representing different commands that go into the internal command
# queue. These are ordered by priority to support using a PriorityQueue.

@total_ordering
class Command:
    PRIORITY = None

    def __eq__(self, other):
        return self.PRIORITY == other.PRIORITY

    def __lt__(self, other):
        return self.PRIORITY < other.PRIORITY


@dataclass
class SourcePositionCommand(Command):
    PRIORITY = 2

    source_id: str
    transponder_id: str
    time: datetime
    callsign: str
    aircrafttype: str | None
    lat: float
    lon: float
    alt: int | None
    alt_gnss: int | None
    heading: float | None
    on_ground: bool


@dataclass
class SourceErrorCommand(Command):
    PRIORITY = 0

    message: str


class SourceDoneCommand(Command):
    PRIORITY = 2


@dataclass
class IngesterStatusCommand(Command):
    PRIORITY = 1

    live: bool


class CompleteCommand(Command):
    PRIORITY = 3


@dataclass
class TrajectoryCommand(Command):
    PRIORITY = 3

    source_id: str


class StopCommand(Command):
    PRIORITY = 0


@dataclass
class RMQCommand(Command):
    PRIORITY = 1

    message: rmq.Message
