from datetime import datetime, timedelta
import os
from typing import Generator

from feder.common import DB, FlightQuery, Trajectory


def get_flights(time1: datetime, time2: datetime) -> list[Trajectory]:
    return list(flight_query(FlightQuery(min_time=time1, max_time=time2)))


def flight_query(query: FlightQuery) -> Generator[Trajectory, None, None]:
    data_dir = os.environ.get('FEDER_DATA_DIR')
    if data_dir is None:
        raise ValueError('environment variable FEDER_DATA_DIR must be set')

    included_dates = [
        query.min_time.date() + timedelta(days=i)
        for i in range((query.max_time.date() - query.min_time.date()).days + 1)
    ]

    for d in included_dates:
        try:
            db = DB(data_dir, d)
        except FileNotFoundError:
            # There's no database file for this particular date.
            continue
        for traj in db.query_flights(query):
            yield traj
