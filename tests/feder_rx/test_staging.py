from conftest import TEST_NOW  # noqa


def test_complete_trajectory_identification(db):
    source_ids = db.complete_source_ids(TEST_NOW - 15 * 60)
    assert source_ids == ['source-0002', 'source-0004']
