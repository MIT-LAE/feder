from datetime import datetime, timedelta, UTC

from feder import FlightQuery, BoundingBox


TSTART = datetime(2025, 5, 22, 18, 15, tzinfo=UTC)
TEND = TSTART + timedelta(minutes=30)


def test_flight_query_1():
    t1 = datetime(2025, 5, 22, 18, 0, tzinfo=UTC)
    t2 = t1 + timedelta(minutes=30)
    query = FlightQuery(t1, t2).time_starts_in()
    assert len(list(query.run())) > 0


def test_flight_query_2():
    t1 = datetime(2025, 5, 22, 18, 0, tzinfo=UTC)
    t2 = t1 + timedelta(minutes=30)
    query = FlightQuery(t1, t2).time_starts_in().with_orig('KPSC')
    assert len(list(query.run())) > 0


def test_flight_query_3():
    t1 = datetime(2025, 5, 22, 18, 0, tzinfo=UTC)
    t2 = t1 + timedelta(minutes=30)
    query = FlightQuery(t1, t2).time_starts_in().with_orig('KBOS')
    assert len(list(query.run())) == 0


def test_spatial_filtering():
    # TODO:
    #  - Plot all flight tracks from test file.
    #  - Identify bounding box where flight bounding box overlaps but flight
    #    track does not.
    #  - Test that flight is included when using bounding box that includes the
    #    track, and excluded when using bounding box that only includes the
    #    bounding box.

    # Bounding boxes for testing: boolean value is whether any of the test
    # flights should be returned by a spatially crossing query, following by
    # min_lon, max_lon, min_lat, max_lat.
    boxes = [
        (False, -115, -110, 37, 43),
        (True, -85, -75, 25, 30),
        (True, -110, -70, 35, 45),
        (False, -79, -77, 40, 41.5),
        (False, -102, -100, 21, 31),
        (True, -119, -113, 35, 38)
    ]

    failed = False
    for box in boxes:
        good, min_lon, max_lon, min_lat, max_lat = box
        q = (
            FlightQuery(TSTART, TEND).
            time_starts_in().spatially_crosses().
            with_bounds(
                min_lon=min_lon, max_lon=max_lon,
                min_lat=min_lat, max_lat=max_lat
            )
        )
        f = list(q.run())
        if good:
            if len(f) == 0:
                failed = True
                print(
                    'expected to find flights crossing box '
                    f'{min_lon}, {max_lon}, {min_lat}, {max_lat}'
                )
        else:
            if len(f) != 0:
                failed = True
                print(
                    'unexpectedly found flights crossing box '
                    f'{min_lon}, {max_lon}, {min_lat}, {max_lat}'
                )
    assert not failed, 'spatial filtering test failed'


def test_waypoint_filtering():
    # This test uses a database file using the new schema.
    bbox = BoundingBox(min_lon=-120, max_lon=-90)
    q1 = FlightQuery(TSTART, TEND).time_starts_in().spatially_crosses().with_bounds(bbox)
    f1 = list(q1.run())
    q2 = q1.filter_waypoints()
    f2 = list(q2.run())
    assert len(f1) > 0
    assert len(f1) == len(f2)
    assert all(len(ff1.points) >= len(ff2.points) for ff1, ff2 in zip(f1, f2))


def test_timezones():
    t1 = datetime(2025, 5, 22, 18, 0, tzinfo=UTC)
    t2 = t1 + timedelta(minutes=30)
    query = FlightQuery(t1, t2).time_starts_in()
    flights =  list(query.run())
    assert flights[0].points[0].time.tzinfo is not None
    # assert all(p.time.tzinfo is not None for p in flights[0].points)


