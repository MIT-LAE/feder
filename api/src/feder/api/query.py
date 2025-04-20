from datetime import datetime, timedelta
import os
from typing import Generator

from feder.common import DB, QueryType, Trajectory


def get_flights(time1: datetime, time2: datetime) -> list[Trajectory]:
    return list(flight_query(min_time=time1, max_time=time2))


def flight_query(
    min_time: datetime, max_time: datetime,
    min_lat: float | None = None, max_lat: float | None = None,
    min_lon: float | None = None, max_lon: float | None = None,
    min_alt: float | None = None, max_alt: float | None = None,
    query_type: QueryType = QueryType.CROSSES
) -> Generator[Trajectory, None, None]:
    data_dir = os.environ.get('FEDER_DATA_DIR')
    if data_dir is None:
        raise ValueError('environment variable FEDER_DATA_DIR must be set')

    included_dates = [
        min_time.date() + timedelta(days=i)
        for i in range((max_time.date() - min_time.date()).days + 1)
    ]

    for d in included_dates:
        try:
            db = DB(data_dir, d)
        except FileNotFoundError:
            # There's no database file for this particular date.
            continue
        for traj in db.query_flights(
                min_time, max_time, min_lat, max_lat,
                min_lon, max_lon, min_alt, max_alt,
                query_type
        ):
            yield traj
