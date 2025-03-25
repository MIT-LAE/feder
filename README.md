# Feder flight data collection system

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

The name: *Feder* is the German word for "feather" and is also supposed to
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
saving a full trajectory record for the flight. As well as being a more
natural way to think about this data for most applications, this also makes
storing and querying the flight data much more efficient than a solution that
deals only with individual position fixes.

## API quickstart

There is a detailed tutorial for the API [here](doc/api-tutorial.md), but to
get started quickly and check that things are working, you can do this in the
shell:

``` shell
pip install /home/mcast/feder/dist/latest.whl
cp /home/mcast/feder/default-config.toml ~/.config/feder.toml
```

and then this in Python:

``` python
from datetime import datetime
from feder.api import get_flights
get_flights(time=datetime(2025, 3, 20, 18, 0), lat=(35.0, 40.0), lon=(-100.0, -95.0), limit=10)
```

This will return a Python array of 10
[`Trajectory`](doc/api-reference.md#Trajectory) objects of flights crossing
the given latitude/longitude bounding box in the one hour window starting at
the given time (all times in UTC).

The API efficiently supports a range of query options and data return formats.
See the [tutorial](doc/api-tutorial.md) or the [API
reference](doc/api-reference.md) for details.
