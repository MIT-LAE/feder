from __future__ import annotations

import dataclasses
from datetime import datetime, timedelta
import os
from typing import Generator

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

    Spatial filtering is disabled by default. If you add spatial bounds with
    `with_bounds`, you must also choose either `spatially_crosses()` or
    `spatially_within()`.

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
        self._bounds: BoundingBox = BoundingBox()

        self._source: DataSource | None = None
        self._callsign: str | None = None
        self._orig: str | None = None
        self._dest: str | None = None

        self._filter_waypoints: bool = False

    def _clone(self) -> FlightQuery:
        """Create a copy of the query. @private"""
        clone = FlightQuery(self._min_time, self._max_time)
        clone._temporal_query_type = self._temporal_query_type
        clone._spatial_query_type = self._spatial_query_type
        clone._bounds = dataclasses.replace(self._bounds)
        clone._source = self._source
        clone._callsign = self._callsign
        clone._orig = self._orig
        clone._dest = self._dest
        clone._filter_waypoints = self._filter_waypoints
        return clone

    def time_intersects(self) -> FlightQuery:
        """Set the temporal query type to "intersects".

        Trajectories are included if they have any points with timestamps
        within the query time range.
        """
        retval = self._clone()
        retval._temporal_query_type = TemporalQueryType.INTERSECTS
        return retval

    def time_within(self) -> FlightQuery:
        """Set the temporal query type to "within".

        Trajectories are included if all their points have timestamps within
        the query time range.
        """
        retval = self._clone()
        retval._temporal_query_type = TemporalQueryType.WITHIN
        return retval

    def time_starts_in(self) -> FlightQuery:
        """Set the temporal query type to "starts in".

        Trajectories are included if their first point has a timestamp within
        the query time range.
        """
        retval = self._clone()
        retval._temporal_query_type = TemporalQueryType.STARTS_IN
        return retval

    def time_ends_in(self) -> FlightQuery:
        """Set the temporal query type to "ends in".

        Trajectories are included if their last point has a timestamp within
        the query time range.
        """
        retval = self._clone()
        retval._temporal_query_type = TemporalQueryType.ENDS_IN
        return retval

    def spatially_crosses(self) -> FlightQuery:
        """Set the spatial query type to "crosses".

        Trajectories are included if any of their points lies within the
        spatial bounding box.
        """
        retval = self._clone()
        retval._spatial_query_type = SpatialQueryType.CROSSES
        return retval

    def spatially_within(self) -> FlightQuery:
        """Set the spatial query type to "within".

        Trajectories are included if all of their points lie within the
        spatial bounding box.
        """
        retval = self._clone()
        retval._spatial_query_type = SpatialQueryType.WITHIN
        return retval

    def with_bounds(
            self,
            bounds: BoundingBox | None = None,
            min_lat: float | None = None,
            max_lat: float | None = None,
            min_lon: float | None = None,
            max_lon: float | None = None,
            min_alt: float | None = None,
            max_alt: float | None = None
    ) -> FlightQuery:
        """Add spatial bounds to the query.

        Bounds can be specified either as a `BoundingBox` value, or as
        individual minimum or maximum limits on latitude, longitude and
        altitude. If both a `BoundingBox` and individual limits are provided,
        the individual limits will override the corresponding values in the
        `BoundingBox`."""
        retval = self._clone()
        if bounds is not None:
            retval._bounds = bounds
        if min_lat is not None:
            retval._bounds.min_lat = float(min_lat)
        if max_lat is not None:
            retval._bounds.max_lat = float(max_lat)
        if min_lon is not None:
            retval._bounds.min_lon = float(min_lon)
        if max_lon is not None:
            retval._bounds.max_lon = float(max_lon)
        if min_alt is not None:
            retval._bounds.min_alt = float(min_alt)
        if max_alt is not None:
            retval._bounds.max_alt = float(max_alt)
        return retval

    def with_callsign(self, callsign: str) -> FlightQuery:
        """Restrict the query to a callsign.

        The `*` character may be used as a wildcard.
        """
        retval = self._clone()
        retval._callsign = callsign
        return retval

    def with_orig(self, orig: str) -> FlightQuery:
        """Restrict the query to an origin airport code.

        The `*` character may be used as a wildcard.
        """
        retval = self._clone()
        retval._orig = orig
        return retval

    def with_dest(self, dest: str) -> FlightQuery:
        """Restrict the query to a destination airport code.

        The `*` character may be used as a wildcard.
        """
        retval = self._clone()
        retval._dest = dest
        return retval

    def from_source(self, source: DataSource) -> FlightQuery:
        """Restrict the query to a specific data source.

        For most time periods, only one data source is in use, but there are
        transitionary periods while we switch over from one data source to
        another. You can narrow down your query to a single source using this
        method. Otherwise you receive all trajectories from all sources in use
        during the time period of your query."""
        retval = self._clone()
        retval._source = source
        return retval

    def filter_waypoints(self) -> FlightQuery:
        """Filter waypoints in the returned trajectories.

        Feder normally returns complete trajectories, including all waypoints.
        This option causes queries to return "partial" trajectories that
        contain only the waypoints that match the exact query conditions. In
        particular, only waypoints within the specified time range and/or
        bounding box will be included. Returned trajectories are marked with
        `partial=True`.
        """
        retval = self._clone()
        retval._filter_waypoints = True
        return retval

    def run(self) -> Generator[Trajectory, None, None]:
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
