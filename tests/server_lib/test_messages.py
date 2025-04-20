from datetime import datetime
import string

from hypothesis import given, settings, strategies as st

import feder.common.models as models
from feder.server import (
    Message, Trajectory, Liveness, LivenessQuery, LivenessResponse
)

from ..conftest import EARLIEST_TIME, LATEST_TIME, point_strategy


@given(st.builds(
    LivenessQuery, source=st.text(min_size=1)))
def test_liveness_query_encoding(q):
    assert q == Message.unpack(q.pack())


@given(st.builds(
    LivenessResponse,
    source=st.text(min_size=1),
    time=st.integers(min_value=EARLIEST_TIME, max_value=LATEST_TIME).map(datetime.fromtimestamp),
    status=st.sampled_from(Liveness)
))
def test_liveness_response_encoding(r):
    assert r == Message.unpack(r.pack())


@given(st.builds(
    Trajectory,
    model=st.builds(
        models.Trajectory,
        id=st.text(min_size=10, max_size=255),
        source=st.sampled_from(models.DataSource),
        transponder_id=st.text(min_size=6, max_size=6, alphabet='0123456789ABCDEF'),
        callsign=st.text(min_size=4, max_size=7, alphabet=string.ascii_uppercase + string.digits),
        aircraft_type=st.sampled_from([None, 'A300', 'B737']),
        points=st.lists(point_strategy, min_size=1, max_size=100))))
@settings(max_examples=1000)
def test_trajectory_encoding(t):
    assert t == Message.unpack(t.pack())
