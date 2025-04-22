from dataclasses import dataclass


@dataclass
class Fix:
    transponder_id: str
    time: int
    orig: str | None
    dest: str | None
    callsign: str
    aircraft_type: str | None
    lat: float
    lon: float
    alt: float | None
    alt_gnss: float | None
    heading: float | None
    on_ground: bool
