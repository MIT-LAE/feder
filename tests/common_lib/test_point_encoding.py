from hypothesis import given, settings

from feder_common.models import Point

from ..conftest import point_strategy


@given(point_strategy)
@settings(max_examples=1000)
def test_round_trip(pt):
    encoded = Point.pack([pt])
    check = Point.unpack(encoded)
    assert len(check) == 1
    assert check[0] == pt
