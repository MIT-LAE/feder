from feder.common import DataSource

from . import Source


class OpenSkySource(Source):
    SOURCE = DataSource.OPENSKY


class OpenSkyStateVectorSource(Source):
    SOURCE = DataSource.OPENSKY_STATE_VECTORS
