from datetime import datetime, timedelta
import os
from typing import Generator, Self

from feder.common import (
    DB, DataSource, BoundingBox, TemporalQueryType, SpatialQueryType, Trajectory
)


class FlightQuery:
    """Flight query class.

    This class has a builder interface to create a flight query with specified
    temporal and spatial constraints, as well as optional filters for data
    source, callsign and origin and destination airport codes.

    A minimum and maximum time are required to create the query, and other
    parameters are set using the chainable methods provided.

    The default temporal query type is "intersects", which means that
    trajectories are included if they have any points with timestamps within
    the specified time range.

    For spatial queries, the default is "crosses", meaning that trajectories
    are included if they have any points that lie within the specified
    bounding box.

    #### Example

    ```
    flights = FlightQuery(time1, time2).bounds(bbox).orig('KBOS').run()
    ```
    """
    def __init__(self, min_time: datetime, max_time: datetime):
        """Initialize a flight query."""

        self._temporal_query_type: TemporalQueryType = TemporalQueryType.INTERSECTS
        self._min_time: datetime = min_time
        self._max_time: datetime = max_time

        self._spatial_query_type: SpatialQueryType | None = None
        self._bounds: BoundingBox | None = None

        self._source: DataSource | None = None
        self._callsign: str | None = None
        self._orig: str | None = None
        self._dest: str | None = None

        self._filter_waypoints: bool = False

    def time_intersects(self) -> Self:
        """Set the temporal query type to "intersects".

        Trajectories are included if they have any points with timestamps
        within the query time range.
        """
        self._temporal_query_type = TemporalQueryType.INTERSECTS
        return self

    def time_within(self) -> Self:
        """Set the temporal query type to "within".

        Trajectories are included if all their points have timestamps within
        the query time range.
        """
        self._temporal_query_type = TemporalQueryType.WITHIN
        return self

    def time_starts_in(self) -> Self:
        """Set the temporal query type to "starts in".

        Trajectories are included if their first point has a timestamp within
        the query time range.
        """
        self._temporal_query_type = TemporalQueryType.STARTS_IN
        return self

    def time_ends_in(self) -> Self:
        """Set the temporal query type to "ends in".

        Trajectories are included if their last point has a timestamp within
        the query time range.
        """
        self._temporal_query_type = TemporalQueryType.ENDS_IN
        return self

    def spatially_crosses(self) -> Self:
        """Set the spatial query type to "crosses".

        Trajectories are included if any of their points lies within the
        spatial bounding box.
        """
        self._spatial_query_type = SpatialQueryType.CROSSES
        return self

    def spatially_within(self) -> Self:
        """Set the spatial query type to "within".

        Trajectories are included if all of their points lie within the
        spatial bounding box.
        """
        self._spatial_query_type = SpatialQueryType.WITHIN
        return self

    def with_bounds(self, bounds: BoundingBox) -> Self:
        """Add spatial bounds to the query."""
        self._bounds = bounds
        return self

    def with_callsign(self, callsign: str) -> Self:
        """Restrict the query to a specific callsign."""
        self._callsign = callsign
        return self

    def with_orig(self, orig: str) -> Self:
        """Restrict the query to a specific origin airport code."""
        self._orig = orig
        return self

    def with_dest(self, dest: str) -> Self:
        """Restrict the query to a specific destination airport code."""
        self._dest = dest
        return self

    def from_source(self, source: DataSource) -> Self:
        """Restrict the query to a specific data source.

        For most time periods, only one data source is in use, but there are
        transitionary periods while we switch over from one data source to
        another. You can narrow down your query to a single source using this
        method. Otherwise you receive all trajectories from all sources in use
        during the time period of your query."""
        self._source = source
        return self

    def filter_waypoints(self) -> Self:
        """Filter waypoints in the returned trajectories.

        Feder normally returns complete trajectories, including all waypoints.
        This option causes queries to return "partial" trajectories that
        contain only the waypoints that match the exact query conditions. In
        particular, only waypoints within the specified time range and/or
        bounding box will be included.
        """
        self._filter_waypoints = True
        return self

    def run(self) -> Generator[Trajectory]:
        """Run the query and yield matching trajectories."""

        data_dir = os.environ.get('FEDER_DATA_DIR')
        if data_dir is None:
            raise ValueError('environment variable FEDER_DATA_DIR must be set')

        included_dates = [
            self._min_time.date() + timedelta(days=i)
            for i in range(
                    -1, (self._max_time.date() - self._min_time.date()).days + 2
            )
        ]

        for d in included_dates:
            try:
                db = DB(data_dir, d)
            except FileNotFoundError:
                # There's no database file for this particular date.
                continue
            for traj in db.query_flights(
                    self._min_time, self._max_time, self._bounds,
                    self._source, self._callsign, self._orig, self._dest,
                    self._temporal_query_type, self._spatial_query_type,
                    self._filter_waypoints
            ):
                yield traj

def get_flights(
        time1: datetime, time2: datetime,
        source: DataSource | None = None
) -> list[Trajectory]:
    """Simplified flight query.

    Queries the database for flights between two times, optionally restricting
    to a selected data source."""

    query = FlightQuery(min_time=time1, max_time=time2)
    if source is not None:
        query = query.from_source(source)
    return list(query.run())
