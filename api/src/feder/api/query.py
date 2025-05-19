import os
from datetime import datetime, timedelta
from typing import Generator

from feder.common import DB, DataSource, QueryType, Trajectory


def get_flights(
        time1: datetime, time2: datetime,
        source: DataSource | None = None
) -> list[Trajectory]:
    """"Simplified flight query.

    Queries the database for flights between two times, optionally restricting
    to a selected data source.

    Parameters
    ----------
    time1 : datetime
        Query start time.
    time2 : datetime
        Query end time.
    source : DataSource | None, default None
        Data source to restrict to.

    Returns
    -------
    list[Trajectory]
        List of trajectories matching the query.

    TODO: Fill the rest of this in - data source handling, etc.
    """
    return list(flight_query(min_time=time1, max_time=time2, source=source))


def flight_query(
    min_time: datetime, max_time: datetime,
    min_lat: float | None = None, max_lat: float | None = None,
    min_lon: float | None = None, max_lon: float | None = None,
    min_alt: float | None = None, max_alt: float | None = None,
    # TODO: Handle source selection properly.
    source: DataSource | None = None,
    callsign: str | None = None,
    orig: str | None = None,
    dest: str | None = None,
    query_type: QueryType = QueryType.CROSSES
) -> Generator[Trajectory, None, None]:
    """Full flight query.

    Query for flight trajectories restricted by a range of conditions (all
    optional).

    TODO: More explanation.
    TODO: Explain (and implement!) data source handling.

    Parmeters
    ---------
    min_time : datetime
        Query start time (required).
    max_time : datetime
        Query end time (required).
    min_lat : float | None, default None
        Minimum latitude (optional).
    max_lat : float | None, default None
        Maximum latitude (optional).
    min_lon : float | None, default None
        Minimum longitude (optional).
    max_lon : float | None, default None
        Maximum longitude (optional).
    min_alt : float | None, default None
        Minimum altitude (optional).
    max_alt : float | None, default None
        Maximum altitude (optional).
    source : DataSource | None, default None
        The data source to restrict to (optional).
    callsign : str | None, default None
        The callsign to restrict to (optional).
    orig : str | None, default None
        The origin airport code to restrict to (optional).
    dest : str | None, default None
        The destination airport code to restrict to (optional).
    query_type : QueryType, default QueryType.CROSSES
        The type of query to perform.

    Yields
    ------
    Trajectory
        The trajectories matching the query.
    """
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
                source, callsign, orig, dest, query_type
        ):
            yield traj
