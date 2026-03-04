"""# Feder API tutorial

Here, I want to demonstrate a few simple ways to use Feder. The results shown
here are based on a very incomplete development database, so they exact values
shown here may look a little strange. It should be enough to give you the
right ideas though!

## Setup

Before starting, you need to install the Feder API in a virtual environment,
either using Conda, uv, Python's built-in `venv` package or some other
mechanism. **You will need Python 3.13 to use Feder.** Using Conda, you might
do something like (we'll use Mamba, because it's faster!):

``` shell
mamba create -n feder-test python==3.13
mamba activate feder-test
mamba install pandas
pip install --extra-index-url https://www.mit.edu/~iross/pypi feder
```

The "`--extra-index-url`" option tells `pip` to look for the Feder package in a
Python package repository hosted on the MIT Athena system, which is where I
keep Feder releases. This avoids problems with trying to install packages from
MIT's private GitHub instance.

Once you've installed Feder, you can just run `python` and do things like
those shown in the following dialog with the Python interpreter. (You can of
course add Feder to an environment that you use in Jupyter notebooks, just
like any other Python package.)

(The following dialog shows results of Python expressions as comments below
the code.)

## Imports and configuration

First, we need some imports. We need dates and times from the Python standard
library, and we'll also show how to interface with Pandas.

The heart of the Feder API is the `feder.query.FlightQuery` class, but we'll
also demonstrate some functions for checking data availability.

``` python
from datetime import date, datetime, timedelta
import os
import pandas as pd
from feder import FlightQuery, BoundingBox
from feder import available_days, available_times, available_sources
```

We need to set the `FEDER_DATA_DIR` environment variable. This can either be
done in the shell (in your `~/.bashrc` file) or, as here, directly in Python.
The location given here is where the Feder data is stored on Hex.

``` python
os.environ['FEDER_DATA_DIR'] = '/home/mcast/data/feder'
```

## Data availability

We can see what days flight data is available for in Feder. The
`feder.available_days` function returns a list of pairs of `date` values
giving ranges of days for which data is available. As mentioned above, I wrote
this using an incomplete development database, so there are only a few days of
data available (here, 2025-03-31 to 2025-04-03).

``` python
available_days()
# [(datetime.date(2025, 3, 31), datetime.date(2025, 4, 3))]
```

Once we know what days data is available for, we can check within a given day
to see what ranges of timestamps flight data is available for. The
`feder.available_times` function returns a list of (start, end) `datetime`
pairs showing the time ranges for which data is available. In this case, the
file for the day used is complete, containing data from 00:00:00 until
23:59:59.

``` python
available_times(date(2025, 4, 1))
# [(datetime.datetime(2025, 4, 1, 0, 0, tzinfo=datetime.timezone.utc),
#   datetime.datetime(2025, 4, 1, 23, 59, 59, tzinfo=datetime.timezone.utc))]
```

During normal operations, the Feder backend servers collect data from a single
data source. However, there may be periods when there is data from multiple
data sources as we switch over from one source to another. The
`feder.available_sources` function allows you to investigate what data sources
are in use for a particular day. The return value is a list of
`feder.DataSource` values. In the example here, the data file examined
contains only data from the FlightAware data source.

``` python
available_sources(date(2025, 4, 1))
# {<DataSource.FLIGHTAWARE: 1>}
```

## Constructing flight queries

Flight queries in Feder are represented by objects of the `feder.FlightQuery`
class. All flight queries require a start and end time, so let's make some
times:

``` python
t1 = datetime(2025, 4, 1, 20, 0)
t2 = t1 + timedelta(minutes=30)
```

Now we construct a query. The `feder.FlightQuery` class has what's called a
"builder" interface, which basically just means that you chain method calls on
the query object to modify the query parameters. Here we make a query using
the times we created, saying that we're interested in flight trajectories
whose *initial* time is within the interval we supply.

``` python
query = FlightQuery(t1, t2).time_starts_in()
```

We then add a condition to require that the flights originate from Boston.

``` python
query = query.with_orig('KBOS')
```

And finally, we add a condition to require that the flight trajectories lie
completely within a given latitude/longitude bounding box. (There are also
filters for callsign, destination and data source.)

``` python
bounds = BoundingBox(min_lat=38, max_lat=46, min_lon=-100, max_lon=-60)
query = query.with_bounds(bounds).spatially_within()
```

With this "builder" approach, we can also make the same query in one go, like
this:

``` python
query = (
    FlightQuery(t1, t2).time_starts_in().
    with_orig('KBOS').
    with_bounds(min_lat=38, max_lat=46, min_lon=-100, max_lon=-60).
    spatially_within()
)
```

(The `bounds` method takes either a `BoundingBox` or a sequence of keyword
arguments for bounding box parameters.)

The above queries will retrieve complete trajectories matching the given
criteria. In some cases, you may want to filter the trajectory waypoints to
include only those individual waypoints that meet the selection criteria. You
can do this by adding the `filter_waypoints` option to your query. The
resulting `feder.Trajectory` values returned from the query will be marked as
"partial" to indicate that the waypoints have been filtered.

## Executing flight queries

Once we have built a flight query, we can execute it by calling its `run`
method. This is a *generator*, to allow for efficient iteration over the
results. If you just want a list of all the flights matching your query, you
can get it like this:

``` python
flights = list(query.run())
```

The result is a normal list: in this case there were 24 flights matching the
query criteria.

``` python
len(flights)
# 24
```

## Flight data

Flight trajectories are returned as values of type `feder.Trajectory`. These
have fields for:

- the data source (`source`),
- the unique flight ID within the data source (`source_id`),
- the aircraft's ADS-B transponder ID (`transponder_id`),
- the ICAO airport codes for the flight origin and destination (`orig`, `dest`),
- the flight callsign (`callsign`),
- the ICAO aircraft type (`aircraft_type`),

plus the flight trajectory as a list of `feder.Point` values, each of which has
time, latitude, longitude, altitude (`alt` for uncorrected pressure altitude in
feet above 1013 hPa and `alt_gnss` for GNSS height in feet relative to the
WGS-84 datum), heading and an "on ground" flag. In addition, `feder.Trajectory`
has a boolean `partial` flag to indicate whether the trajectory is the result
of a query using waypoint filtering or not.

Here's part of the data for one flight:

``` python
flights[0]
# Trajectory(
#   source_id='RPA5730-1743310379-airline-1555p',
#   source=<DataSource.FLIGHTAWARE: 1>,
#   transponder_id='A21833',
#   orig='KBOS',
#   dest='KLGA',
#   callsign='RPA5730',
#   aircraft_type='E75L',
#   partial=False,
#   points=[
#     Point(time=datetime.datetime(2025, 4, 1, 18, 7, 13, tzinfo=datetime.timezone.utc), lon=-71.013, lat=42.37, alt=725.0, alt_gnss=600.0, heading=314.0, on_ground=False),
#     Point(time=datetime.datetime(2025, 4, 1, 18, 8, 17, tzinfo=datetime.timezone.utc), lon=-71.063, lat=42.398, alt=3275.0, alt_gnss=3125.0, heading=300.0, on_ground=False),
#     Point(time=datetime.datetime(2025, 4, 1, 18, 9, 28, tzinfo=datetime.timezone.utc), lon=-71.152, lat=42.385, alt=6550.0, alt_gnss=6325.0, heading=234.0, on_ground=False),
#     Point(time=datetime.datetime(2025, 4, 1, 18, 9, 58, tzinfo=datetime.timezone.utc), lon=-71.192, lat=42.364, alt=7975.0, alt_gnss=7725.0, heading=235.0, on_ground=False),
#     Point(time=datetime.datetime(2025, 4, 1, 18, 10, 58, tzinfo=datetime.timezone.utc), lon=-71.276, lat=42.32, alt=10875.0, alt_gnss=10675.0, heading=236.0, on_ground=False),
#     Point(time=datetime.datetime(2025, 4, 1, 18, 12, 25, tzinfo=datetime.timezone.utc), lon=-71.44, lat=42.273, alt=13900.0, alt_gnss=13725.0, heading=252.0, on_ground=False),
#     Point(time=datetime.datetime(2025, 4, 1, 18, 13, 25, tzinfo=datetime.timezone.utc), lon=-71.555, lat=42.243, alt=14050.0, alt_gnss=13875.0, heading=251.0, on_ground=False),
#     Point(time=datetime.datetime(2025, 4, 1, 18, 14, 36, tzinfo=datetime.timezone.utc), lon=-71.696, lat=42.206, alt=14050.0, alt_gnss=13875.0, heading=251.0, on_ground=False),
#     Point(time=datetime.datetime(2025, 4, 1, 18, 15, 6, tzinfo=datetime.timezone.utc), lon=-71.753, lat=42.191, alt=14050.0, alt_gnss=13875.0, heading=251.0, on_ground=False),
#     Point(time=datetime.datetime(2025, 4, 1, 18, 16, 6, tzinfo=datetime.timezone.utc), lon=-71.871, lat=42.16, alt=14050.0, alt_gnss=13875.0, heading=251.0, on_ground=False),
#     ...
#   ]
# )
```

## Where are my dataframes, you barbarian?

If you prefer to have your trajectory data as a Pandas data frame, you can
easily convert the list of `feder.Point` values in the trajectory data into a
data frame, like this:

``` python
pd.DataFrame(flights[0].points)
#                         time     lon     lat      alt  alt_gnss  heading  on_ground
# 0  2025-04-01 18:07:13+00:00 -71.013  42.370    725.0     600.0    314.0      False
# 1  2025-04-01 18:08:17+00:00 -71.063  42.398   3275.0    3125.0    300.0      False
# 2  2025-04-01 18:09:28+00:00 -71.152  42.385   6550.0    6325.0    234.0      False
# 3  2025-04-01 18:09:58+00:00 -71.192  42.364   7975.0    7725.0    235.0      False
# 4  2025-04-01 18:10:58+00:00 -71.276  42.320  10875.0   10675.0    236.0      False
# ...
# 44 2025-04-01 18:54:48+00:00 -73.972  40.662   2925.0    2850.0     35.0      False
# 45 2025-04-01 18:55:14+00:00 -73.957  40.680   2900.0    2825.0     34.0      False
# 46 2025-04-01 18:56:25+00:00 -73.906  40.719   2075.0    2025.0     68.0      False
# 47 2025-04-01 18:57:29+00:00 -73.849  40.730   1150.0    1100.0     70.0      False
# 48 2025-04-01 18:58:33+00:00 -73.839  40.762    300.0     275.0    316.0      False
```

## Iterating over trajectories

Because the `feder.FlightQuery.run` method is a generator method, you can use
it to iterate over query results one by one. This can be more efficient for
large query result sets than asking for the whole list of results at once.

Here, we iterate over the results from the query above, displaying origin and
destination and total flight time:

``` python
for flight in query.run():
    print(flight.orig, flight.dest, flight.points[-1].time - flight.points[0].time)
# KBOS KLGA 0:51:20
# KBOS KTEB 0:51:16
# KBOS KEWR 0:54:36
# KBOS KLGA 0:45:18
# KBOS KJFK 0:47:12
# KBOS CYTZ 1:40:12
# KBOS KROC 1:00:57
# KBOS KDCA 1:27:42
# KBOS CYTZ 1:40:12
# KBOS KMSS 1:12:15
# KBOS KDCA 1:18:25
# KBOS KMSS 1:12:15
# KBOS KJFK 0:47:12
# KBOS KDCA 1:27:42
# KBOS KACK 0:41:17
# KBOS KACK 0:41:17
# KBOS KTEB 0:51:16
# KBOS KEWR 0:54:36
# KBOS KLGA 0:45:18
# KBOS KLGA 0:51:20
# KBOS KROC 1:00:57
# KBOS KDCA 1:18:25
# KBOS KIND 2:11:43
# KBOS KIND 2:11:43
```

## Combining queries

Here is a slightly more complex example. Suppose that we want to look at
flights crossing a region in the horizontal (defined by a bounding box), but we
want to split the trajectories into flight level "slices".

First we choose a time range and a bounding box for the region we're interested
in:

```python
TSTART = datetime(2025, 5, 22, 0, 0, tzinfo=UTC)
TEND = TSTART + timedelta(hours=24)

bbox = BoundingBox(min_lon=-100, max_lon=-80, min_lat=30, max_lat=45)
```

Now we start with a query without altitude information. We use the
`filter_waypoints` method so that we only get the trajectory points within the
additional altitude bounds we're going to impose.

```python
query = (
    FlightQuery(TSTART, TEND).time_starts_in().
        with_bounds(bounds=bbox).spatially_crosses().
        filter_waypoints()
)
```

Now we can iterate over flight level "slices" of the trajectories by adding
altitude bounds. Here, we don't do anything useful with the information, we
just print out the number of flights and total number of trajectory points in
each slice:

```python
fl_boundaries = np.arange(250, 410, 10)
for fl_min, fl_max in zip(fl_boundaries[:-1], fl_boundaries[1:]):
    sub_query = query.with_bounds(min_alt=fl_min * 100, max_alt=fl_max * 100)
    flights = list(sub_query.run())
    print(fl_min, fl_max, len(flights), sum(len(f.points) for f in flights))
```

"""
