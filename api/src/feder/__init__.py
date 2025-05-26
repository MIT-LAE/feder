"""
.. include:: ../../../README.md
"""

from .query import get_flights, flight_query  # noqa
from .available import available_days, available_times  # noqa
from .common.models import DataSource, Point, Trajectory  # noqa
from .common.db import QueryType  # noqa
from .common.version import get_feder_version  # noqa


__version__ = '0.1.7'
