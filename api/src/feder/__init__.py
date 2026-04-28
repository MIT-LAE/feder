"""# Feder flight data collection system

Feder is a system for collecting flight data from a range of sources into a
consolidated database for applications that need easy access to historical
flight data for statistical or flight attribution purposes. It comes in two
main parts:

1. An API (published as a Python package) for querying the collected data.
   This data is stored in SQLite3 database files, and the API provides a
   simple interface for making efficient queries to these database files.

2. A set of server processes that collaborate to collect and integrate data
   from different flight data sources, plus infrastructure to deploy these
   server processes so that they work correctly together.

**The name**: *Feder* is the German word for "feather" and is also supposed to
give the feeling of **F**light **D**ata **R**ecorder. It's short and unique
and easy to say ("fay-der", more or less).

## Concepts: positions vs. trajectories

Flight data is usually provided by data sources like FlightAware's Firehose or
the OpenSky network as individual flight *state vectors*, encoding a single
snapshot in time of a flight's position. These state vectors are not *exactly*
individual ADS-B position fixes, because most data provides filter the raw
ADS-B fixes to reduce data volumes, but they do represent individual position
fixes at a single point in time. For most applications, it is more useful to
provide access to *flight trajectories*, i.e. all the position fixes for a
given flight (possibly restricted to a selected temporal or spatial domain).

The Feder API works in terms of these trajectories (or parts of trajectories),
and the server processes generate trajectory records for full flights by
collecting position fixes until it appears that a flight is complete and then
saving a full trajectory record for the flight. As well as being a more natural
way to think about this data for most applications, this also makes storing and
querying the flight data much more efficient than a solution that deals only
with individual position fixes. Applications that need to work with exactly
those waypoints in a trajectory that lie within a given temporal or spatial
range may use the waypoint filtering option of the `feder.FlightQuery` class.

## Data units

All data values are reported from Feder exactly as they come from the flight
data providers. [Experiments have
shown](https://github.mit.edu/iross/flight-data-project/blob/main/experiments/flight-data-comparison/flight-data-comparison.pdf)
that the different data providers (at least FlightAware and the Contrails API)
provide comparable data, in the sense that the data is exactly what is sent by
the aircraft's ADS-B transponder. (FlightAware does some data cleaning using
other sources, but they make the result look as though it came from the ADS-B
transponder, just with any anomalous values being cleaned up.)

For the user of data from Feder, what this means is:

 - Latitude and longitude values are "normal" WGS-84 latitudes and longitudes;

 - The `alt` field of `feder.Point` contains uncorrected [pressure
   altitude](https://en.wikipedia.org/wiki/Pressure_altitude) in feet
   (relative to 1013 hPa) as reported in ADS-B messages;

 - The `alt_gnss` field of `feder.Point` contains GNSS height in feet relative
   to the WGS-84 datum.


## API quickstart

There is a detailed tutorial for the API [here](feder/tutorial.html), but to
get started quickly and check that things are working, you can do this in the
shell:

``` shell
pip install --extra-index-url https://www.mit.edu/~iross/pypi feder
export FEDER_DATA_DIR=/home/mcast/data/feder
```

and then this in Python:

``` python
from datetime import UTC, datetime, timedelta
from feder import FlightQuery
t1 = datetime(2025, 5, 22, 20, 0, tzinfo=UTC)
t2 = t1 + timedelta(minutes=30)
flights = list(FlightQuery(t1, t2).with_orig('KBOS').run())

```

This will return a Python list of [`Trajectory`](#Trajectory) objects for
flights originating at KBOS whose trajectories intersect the 30-minute window
starting at the given time. All query times should be provided as UTC
`datetime` values.

The API efficiently supports a range of query options and data return formats.
See the [tutorial](feder/tutorial.html) or the API reference documentation
below for details.

## API documentation

"""

from .query import get_flights, FlightQuery  # noqa
from .available import available_days, available_times, available_sources  # noqa
from .common.models import DataSource, Point, Trajectory  # noqa
from .common.db import BoundingBox, TemporalQueryType, SpatialQueryType  # noqa
from .common.version import get_feder_version  # noqa

# The following is needed just to build the documentation.
import feder.tutorial  # noqa

__all__ = [
    'get_flights',
    'FlightQuery',
    'available_days',
    'available_times',
    'available_sources',
    'DataSource',
    'Point',
    'Trajectory',
    'BoundingBox',
    'TemporalQueryType',
    'SpatialQueryType',
    'get_feder_version',
    'tutorial'
]

__version__ = '1.0.0'
