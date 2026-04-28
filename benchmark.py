import math
import os
from datetime import datetime, timedelta

from feder import FlightQuery, BoundingBox


os.environ['FEDER_DATA_DIR'] = '/data2/feder/main'

t1 = datetime(2025, 5, 22, 20, 0)
t2 = t1 + timedelta(minutes=60)

# Approximate bounding box calculation.
center_lat = 42.360278
center_lon = -71.057778
radius = 50.0 # km
dlat = radius / 111.0
dlon = radius / 111.0 * math.cos(math.radians(center_lat))
extent = BoundingBox(
    min_lat=center_lat - dlat,
    max_lat=center_lat + dlat,
    min_lon=center_lon - dlon,
    max_lon=center_lon + dlon
)

query = FlightQuery(t1, t2).spatially_crosses().with_bounds(extent).filter_waypoints()

repeats = 10

def run():
    flights = list(query.run())
    tstart = datetime.now()
    for i in range(repeats):
        flights = list(query.run())
    tend = datetime.now()
    print(f"Found {len(flights)} flights ({repeats} calls) in {tend - tstart} (with warm-up)")


if __name__ == "__main__":
    run()
