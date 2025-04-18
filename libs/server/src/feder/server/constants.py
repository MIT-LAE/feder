from enum import Enum


class DataSource(Enum):
    FLIGHTAWARE = 1
    CONTRAILS_API = 2
    OPENSKY = 3
    OPENSKY_STATE_VECTORS = 4

    def __str__(self):
        return self.name.lower().replace('_', ' ')
