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
pip install /home/mcast/feder/dist/feder-0.1.16-py3-none-any.whl
```

You install the `feder` package itself from a "wheel file". This is just a
packaged-up version of the Feder API code: when you install a package from
PyPI using `pip`, the thing you actually download to install is usually a
wheel file. In this case, I don't want to upload Feder to PyPI, so we just
keep the wheel file locally on Hex for you to install.

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
data available (here, 2025-05-21 to 2025-05-25).

``` python
available_days()
# [(datetime.date(2025, 5, 21), datetime.date(2025, 5, 25))]
```

Once we know what days data is available for, we can check within a given day
to see what ranges of timestamps flight data is available for. The
`feder.available_times` function returns a list of (start, end) `datetime`
pairs showing the time ranges for which data is available. In this case, the
file for the day used is incomplete, containing data only from 19:57:20 until
22:03:12.

``` python
available_times(date(2025, 5, 22))
# [(datetime.datetime(2025, 5, 22, 19, 57, 20),
#   datetime.datetime(2025, 5, 22, 22, 3, 12))]
```

During normal operations, the Feder backend servers collect data from a single
data source. However, there may be periods when there is data from multiple
data sources as we switch over from one source to another. The
`feder.available_sources` function allows you to investigate what data sources
are in use for a particular day. The return value is a list of
`feder.DataSource` values. In the example here, the data file examined
contains only data from the Contrails API data source.

``` python
available_sources(date(2025, 5, 22))
# {<DataSource.CONTRAILS_API: 2>}
```

## Constructing flight queries

Flight queries in Feder are represented by objects of the `feder.FlightQuery`
class. All flight queries require a start and end time, so let's make some
times:

``` python
t1 = datetime(2025, 5, 22, 20, 0)
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
bounds = BoundingBox(min_lat=38, max_lat=46, min_lon=-100, max_lon=-60)
query = (
    FlightQuery(t1, t2).time_starts_in().
    with_orig('KBOS').
    with_bounds(bounds).spatially_within()
)
```

## Executing flight queries

Once we have built a flight query, we can execute it by calling its `run`
method. This is a *generator*, to allow for efficient iteration over the
results. If you just want a list of all the flights matching your query, you
can get it like this:

``` python
flights = list(query.run())
```

The result is a normal list: in this case there were 8 flights matching the
query criteria.

``` python
len(flights)
# 8
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

plus the flight trajectory as a list of `feder.Point` values, each of which
has time, latitude, longitude, altitude (`alt` for uncorrected pressure
altitude in feet above 1013 hPa and `alt_gnss` for GNSS height in feet
relative to the WGS-84 datum), heading and an "on ground" flag.

Here's part of the data for one flight:

``` python
flights[0]
# Trajectory(
#   source_id='ab238587-3419-412e-9fd3-74120f32e65b',
#   source=<DataSource.CONTRAILS_API: 2>,
#   transponder_id='A1ACDB',
#   orig='KBOS',
#   dest='KCVG',
#   callsign='RPA5789',
#   aircraft_type='E75S',
#   points=[
#     Point(time=datetime.datetime(2025, 5, 22, 20, 7, 1), lon=-71.022, lat=42.363, alt=12.0, alt_gnss=None, heading=None, on_ground=False),
#     Point(time=datetime.datetime(2025, 5, 22, 20, 7, 36), lon=-71.023, lat=42.363, alt=12.0, alt_gnss=None, heading=None, on_ground=False),
#     Point(time=datetime.datetime(2025, 5, 22, 20, 8, 25), lon=-71.023, lat=42.363, alt=0.0, alt_gnss=None, heading=None, on_ground=False),
#     Point(time=datetime.datetime(2025, 5, 22, 20, 9, 44), lon=-71.022, lat=42.36, alt=2850.0, alt_gnss=2850.0, heading=None, on_ground=False),
#     Point(time=datetime.datetime(2025, 5, 22, 20, 10, 2), lon=-71.02, lat=42.359, alt=0.0, alt_gnss=None, heading=None, on_ground=False),
#     Point(time=datetime.datetime(2025, 5, 22, 20, 10, 59), lon=-71.016, lat=42.357, alt=0.0, alt_gnss=None, heading=None, on_ground=False),
#     Point(time=datetime.datetime(2025, 5, 22, 20, 11, 3), lon=-71.016, lat=42.357, alt=0.0, alt_gnss=None, heading=None, on_ground=False),
#     Point(time=datetime.datetime(2025, 5, 22, 20, 11, 55), lon=-71.015, lat=42.354, alt=0.0, alt_gnss=None, heading=None, on_ground=False),
#     Point(time=datetime.datetime(2025, 5, 22, 20, 12, 1), lon=-71.015, lat=42.354, alt=0.0, alt_gnss=None, heading=None, on_ground=False),
#     Point(time=datetime.datetime(2025, 5, 22, 20, 12, 36), lon=-71.014, lat=42.355, alt=0.0, alt_gnss=None, heading=None, on_ground=False),
#     Point(time=datetime.datetime(2025, 5, 22, 20, 14, 29), lon=-71.014, lat=42.355, alt=0.0, alt_gnss=None, heading=None, on_ground=False),
#     Point(time=datetime.datetime(2025, 5, 22, 20, 14, 57), lon=-71.013, lat=42.356, alt=0.0, alt_gnss=None, heading=None, on_ground=False),
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
#                    time     lon     lat     alt  alt_gnss heading  on_ground
# 0   2025-05-22 20:07:01 -71.022  42.363    12.0       NaN    None      False
# 1   2025-05-22 20:07:36 -71.023  42.363    12.0       NaN    None      False
# 2   2025-05-22 20:08:25 -71.023  42.363     0.0       NaN    None      False
# 3   2025-05-22 20:09:44 -71.022  42.360  2850.0    2850.0    None      False
# 4   2025-05-22 20:10:02 -71.020  42.359     0.0       NaN    None      False
# ..                  ...     ...     ...     ...       ...     ...        ...
# 230 2025-05-22 21:58:00 -84.474  39.053  3600.0    3550.0    None      False
# 231 2025-05-22 21:58:58 -84.534  39.046  2675.0    2600.0    None      False
# 232 2025-05-22 21:59:00 -84.538  39.046  2625.0    2550.0    None      False
# 233 2025-05-22 21:59:54 -84.583  39.046  1925.0    1850.0    None      False
# 234 2025-05-22 21:59:56 -84.584  39.046  1925.0    1850.0    None      False
#
# [235 rows x 7 columns]
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
# KBOS KCVG 1:52:55
# KBOS KTEB 1:01:54
# KBOS KMSY 1:35:26
# KBOS KLGA 1:20:31
# KBOS KDFW 1:43:00
# KBOS KBWI 1:40:28
# KBOS KDEN 1:41:20
# KBOS KLAS 1:36:05
```

"""
