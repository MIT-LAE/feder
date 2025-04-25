import bz2
from datetime import datetime

from hypothesis import given, settings, strategies as st

from feder.server import (
    Message,
    Liveness, LivenessQuery, LivenessResponse,
    Trajectory, TrajectoryBatch
)

from ..conftest import (
    EARLIEST_TIME, LATEST_TIME,
    short_string_strategy, trajectory_strategy
)


@given(st.builds(LivenessQuery, source=short_string_strategy))
@settings(max_examples=1000)
def test_liveness_query_encoding(q):
    assert q == Message.unpack(q.pack())


@given(st.builds(
    LivenessResponse,
    source=short_string_strategy,
    time=st.integers(min_value=EARLIEST_TIME, max_value=LATEST_TIME).map(datetime.fromtimestamp),
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
    trajectories=st.lists(st.builds(Trajectory, model=trajectory_strategy))
))
@settings(max_examples=200)
def test_trajectory_batch_encoding(t):
    assert t == Message.unpack(t.pack())
