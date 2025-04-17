from dataclasses import dataclass
from datetime import datetime


@dataclass
class Point:
    time: datetime
    lon: float
    lat: float
    alt: float
    alt_gnss: float | None
    on_ground: bool


@dataclass
class Trajectory:
    id: str
    source: str
    transponder_id: str
    callsign: str
    aircraft_type: str | None
    points: list[Point]
