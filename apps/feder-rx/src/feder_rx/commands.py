from dataclasses import dataclass
from datetime import datetime
import functools

import feder_server.rmq as rmq
from feder_server.messages import IngesterLivenessResponse


# Classes representing different commands that go into the internal command
# queue.

@functools.total_ordering
class Command:
    def priority(self) -> rmq.Message.Priority:
        return rmq.Message.Priority.MEDIUM

    def __eq__(self, other):
        return self.priority() == other.priority()

    def __lt__(self, other):
        if isinstance(self, RMQCommand) and isinstance(other, RMQCommand):
            return self.message < other.message
        else:
            return self.priority() < other.priority()


class StopCommand(Command):
    def priority(self) -> rmq.Message.Priority:
        return rmq.Message.Priority.MAXIMUM


@dataclass
class SourceErrorCommand(Command):
    message: str
    stop: bool

    def priority(self) -> rmq.Message.Priority:
        return rmq.Message.Priority.HIGH


@dataclass
class SourceDoneCommand(Command):
    latest_time: datetime

    def priority(self) -> rmq.Message.Priority:
        # Make sure this gets processed late.
        return rmq.Message.Priority.LOW


@dataclass
class IngesterStatusCommand(Command):
    response: IngesterLivenessResponse | None
    response_received: datetime

    def priority(self) -> rmq.Message.Priority:
        return rmq.Message.Priority.HIGH


@dataclass
class RMQCommand(Command):
    message: rmq.Message

    def priority(self) -> rmq.Message.Priority:
        return rmq.Message.Priority.HIGH


@dataclass
class SourcePositionCommand(Command):
    source_id: str
    transponder_id: str
    time: datetime
    orig: str | None
    dest: str | None
    callsign: str
    aircraft_type: str | None
    lat: float
    lon: float
    alt: int | None
    alt_gnss: int | None
    heading: float | None
    on_ground: bool


@dataclass
class BatchSourcePositionCommand(Command):
    source_ids: list[str]
    transponder_ids: list[str]
    times: list[datetime]
    origs: list[str | None]
    dests: list[str | None]
    callsigns: list[str]
    aircraft_types: list[str | None]
    lats: list[float]
    lons: list[float]
    alts: list[int | None]
    alts_gnss: list[int | None]
    headings: list[float | None]
    on_grounds: list[bool]
