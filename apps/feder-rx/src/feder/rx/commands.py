from dataclasses import dataclass
from datetime import datetime

import feder.server.rmq as rmq


# Classes representing different commands that go into the internal command
# queue.

class Command:
    ...


@dataclass
class SourceErrorCommand(Command):
    message: str


@dataclass
class SourceDoneCommand(Command):
    latest_time: datetime


@dataclass
class IngesterStatusCommand(Command):
    live: bool


class CompleteCommand(Command):
    ...


class StopCommand(Command):
    ...


@dataclass
class RMQCommand(Command):
    message: rmq.Message


@dataclass
class SourcePositionCommand:
    source_id: str
    transponder_id: str
    time: datetime
    orig: list[str | None]
    dest: list[str | None]
    callsign: str
    aircraft_type: str | None
    lat: float
    lon: float
    alt: int | None
    alt_gnss: int | None
    heading: float | None
    on_ground: bool


@dataclass
class BatchSourcePositionCommand:
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
