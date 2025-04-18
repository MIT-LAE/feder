from datetime import datetime

from hypothesis import given, settings, strategies as st

from feder.common.models import Point

EARLIEST = int(datetime(2000, 1, 1).timestamp())
LATEST = int(datetime(2030, 1, 1).timestamp())


def milli(x):
    return round(x, 3)


@given(st.builds(
    Point,
    time=st.integers(min_value=EARLIEST, max_value=LATEST).map(datetime.fromtimestamp),
    lon=st.floats(min_value=-180.0, max_value=180.0).map(milli),
    lat=st.floats(min_value=-90.0, max_value=90.0).map(milli),
    alt=st.floats(min_value=-5000.0, max_value=100000.0).map(milli),
    alt_gnss=st.none(), heading=st.none(), on_ground=st.booleans()))
@settings(max_examples=1000)
def test_round_trip(pt):
    encoded = Point.pack([pt])
    check = Point.unpack(encoded)
    assert len(check) == 1
    assert check[0] == pt
