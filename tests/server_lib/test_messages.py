from datetime import datetime

from feder.server.messages import (
    Message, Trajectory, Liveness, LivenessQuery, LivenessResponse
)


def test_liveness_query_encoding():
    q = LivenessQuery(source='test-source')
    packed = q.pack()
    check = Message.unpack(packed)
    assert q == check


def test_liveness_response_encoding():
    check_time = datetime(2025, 4, 18, 10, 27)
    q = LivenessResponse(
        source='test-source',
        time=check_time,
        status=Liveness.OK
    )
    packed = q.pack()
    check = Message.unpack(packed)
    assert q == check
