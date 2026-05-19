from datetime import date, datetime, UTC

import pytest

from feder import (
    TrajectoryArray, TrajectoryArrayBatch, stream_trajectories,
    stream_trajectory_arrays
)
from feder.common import DB


DAY = date(2025, 5, 22)


def test_stream_trajectories_returns_trajectories():
    trajectories = list(stream_trajectories(DAY))
    assert len(trajectories) > 0
    assert all(not traj.partial for traj in trajectories)


def test_stream_trajectories_count_matches_db_size():
    db = DB('tests/api/data', DAY)
    try:
        assert len(list(stream_trajectories(DAY))) == db.size()
    finally:
        db.close()


def test_stream_trajectories_small_batch_returns_all_rows():
    expected = len(list(stream_trajectories(DAY)))
    assert len(list(stream_trajectories(DAY, batch_size=1))) == expected


@pytest.mark.parametrize(
    'bad_day',
    [
        datetime(2025, 5, 22, tzinfo=UTC),
        1_747_936_800,
        '2025-05-22',
    ]
)
def test_stream_trajectories_rejects_non_date_days(bad_day):
    with pytest.raises(TypeError, match='day must be a datetime.date'):
        list(stream_trajectories(bad_day))


def test_stream_trajectories_missing_day_raises_file_not_found():
    with pytest.raises(FileNotFoundError):
        list(stream_trajectories(date(1999, 1, 1)))


def test_stream_trajectories_rejects_invalid_batch_size():
    with pytest.raises(ValueError, match='batch_size must be at least 1'):
        list(stream_trajectories(DAY, batch_size=0))


def test_stream_trajectory_arrays_returns_trajectory_array_batches():
    batches = list(stream_trajectory_arrays(DAY))
    trajectories = [traj for batch in batches for traj in batch.trajectories]
    assert len(batches) > 0
    assert all(isinstance(batch, TrajectoryArrayBatch) for batch in batches)
    assert all(batch.day == DAY for batch in batches)
    assert all(batch.row_count == batch.trajectory_count for batch in batches)
    assert all(
        batch.point_count == sum(len(traj.points) for traj in batch.trajectories)
        for batch in batches
    )
    assert all(isinstance(traj, TrajectoryArray) for traj in trajectories)
    assert all(not traj.partial for traj in trajectories)


def test_stream_trajectory_arrays_count_matches_stream_trajectories():
    batches = list(stream_trajectory_arrays(DAY))
    streamed_count = sum(batch.trajectory_count for batch in batches)
    assert streamed_count == len(list(stream_trajectories(DAY)))


def test_stream_trajectory_arrays_small_batch_returns_all_rows():
    expected = sum(
        batch.trajectory_count for batch in stream_trajectory_arrays(DAY)
    )
    batches = list(stream_trajectory_arrays(DAY, batch_size=1))
    assert len(batches) == expected
    assert all(batch.row_count == 1 for batch in batches)


@pytest.mark.parametrize(
    'bad_day',
    [
        datetime(2025, 5, 22, tzinfo=UTC),
        1_747_936_800,
        '2025-05-22',
    ]
)
def test_stream_trajectory_arrays_rejects_non_date_days(bad_day):
    with pytest.raises(TypeError, match='day must be a datetime.date'):
        list(stream_trajectory_arrays(bad_day))


def test_stream_trajectory_arrays_missing_day_raises_file_not_found():
    with pytest.raises(FileNotFoundError):
        list(stream_trajectory_arrays(date(1999, 1, 1)))


def test_stream_trajectory_arrays_rejects_invalid_batch_size():
    with pytest.raises(ValueError, match='batch_size must be at least 1'):
        list(stream_trajectory_arrays(DAY, batch_size=0))
