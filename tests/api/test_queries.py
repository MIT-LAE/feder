from datetime import datetime, timedelta

from feder import FlightQuery, BoundingBox


TSTART = datetime(2025, 5, 22, 20, 15)
TEND = TSTART + timedelta(minutes=30)


def test_flight_query_1():
    t1 = datetime(2025, 5, 22, 20, 0)
    t2 = t1 + timedelta(minutes=30)
    query = FlightQuery(t1, t2).time_starts_in()
    assert len(list(query.run())) > 0


def test_flight_query_2():
    t1 = datetime(2025, 5, 22, 20, 0)
    t2 = t1 + timedelta(minutes=30)
    query = FlightQuery(t1, t2).time_starts_in().with_orig('KPSC')
    assert len(list(query.run())) > 0


def test_flight_query_3():
    t1 = datetime(2025, 5, 22, 20, 0)
    t2 = t1 + timedelta(minutes=30)
    query = FlightQuery(t1, t2).time_starts_in().with_orig('KBOS')
    assert len(list(query.run())) == 0


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
    t1 = datetime(2025, 5, 22, 20, 0)
    t2 = t1 + timedelta(minutes=30)
    query = FlightQuery(t1, t2).time_starts_in()
    flights =  list(query.run())
    assert flights[0].points[0].time.tzinfo is not None
    # assert all(p.time.tzinfo is not None for p in flights[0].points)


