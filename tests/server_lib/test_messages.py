import bz2
from datetime import datetime, timezone

from hypothesis import given, settings, strategies as st

from feder_server import (
    Message,
    Liveness, IngesterLivenessQuery, IngesterLivenessResponse,
    Trajectory, TrajectoryBatch
)

from ..conftest import (
    EARLIEST_TIME, LATEST_TIME,
    short_string_strategy, trajectory_strategy
)


@given(st.builds(IngesterLivenessQuery, source=short_string_strategy))
@settings(max_examples=1000)
def test_liveness_query_encoding(q):
    assert q == Message.unpack(q.pack())


@given(st.builds(
    IngesterLivenessResponse,
    source=short_string_strategy,
    time=st.integers(min_value=EARLIEST_TIME, max_value=LATEST_TIME).map(
        lambda ts: datetime.fromtimestamp(ts, tz=timezone.utc)),
    status=st.sampled_from(Liveness)
))
@settings(max_examples=1000)
def test_liveness_response_encoding(r):
    assert r == Message.unpack(r.pack())


@given(st.builds(
    Trajectory,
    model=trajectory_strategy
))
@settings(max_examples=1000)
def test_trajectory_encoding(t):
    assert t == Message.unpack(t.pack())


@given(st.builds(
    TrajectoryBatch,
    trajectories=st.lists(st.builds(Trajectory, model=trajectory_strategy)),
    source=short_string_strategy,
    trajectory_count=st.integers(min_value=0, max_value=10000)
))
@settings(max_examples=200)
def test_trajectory_batch_encoding(t):
    assert t == Message.unpack(t.pack())
