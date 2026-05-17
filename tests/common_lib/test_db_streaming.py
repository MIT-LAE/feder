from datetime import date
from pathlib import Path

import pytest

from feder_common import DB


DATA_DIR = Path(__file__).parents[1] / 'api' / 'data'
REF_DATE = date(2025, 5, 22)


def test_stream_trajectories_returns_all_rows():
    db = DB(str(DATA_DIR), REF_DATE)
    try:
        trajectories = list(db.stream_trajectories())
        assert len(trajectories) == db.size()
        assert len(trajectories) > 0
        assert all(not traj.partial for traj in trajectories)
    finally:
        db.close()


def test_stream_trajectories_small_batch_returns_all_rows():
    db = DB(str(DATA_DIR), REF_DATE)
    try:
        assert len(list(db.stream_trajectories(batch_size=1))) == db.size()
        assert len(list(db.stream_trajectories(batch_size=2))) == db.size()
    finally:
        db.close()


@pytest.mark.parametrize('batch_size', [0, -1])
def test_stream_trajectories_rejects_invalid_batch_size(batch_size):
    db = DB(str(DATA_DIR), REF_DATE)
    try:
        with pytest.raises(ValueError, match='batch_size must be at least 1'):
            list(db.stream_trajectories(batch_size=batch_size))
    finally:
        db.close()
