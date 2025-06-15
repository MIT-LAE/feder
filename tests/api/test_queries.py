from datetime import datetime, timedelta

from feder import FlightQuery


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


def test_timezones():
    t1 = datetime(2025, 5, 22, 20, 0)
    t2 = t1 + timedelta(minutes=30)
    query = FlightQuery(t1, t2).time_starts_in()
    flights =  list(query.run())
    assert flights[0].points[0].time.tzinfo is not None
    # assert all(p.time.tzinfo is not None for p in flights[0].points)


