import pytest

from feder_ingest import DBCache


def test_db_cache_requires_distinct_unnested_path_roots(tmp_path):
    data = tmp_path / 'data'
    data.mkdir()

    with pytest.raises(ValueError):
        DBCache(str(data), str(data), str(tmp_path / 'scratch'))

    with pytest.raises(ValueError):
        DBCache(str(data), str(tmp_path / 'staging'), str(data / 'scratch'))


def test_db_cache_requires_existing_data_dir_and_creates_private_dirs(tmp_path):
    data = tmp_path / 'data'
    staging = tmp_path / 'staging'
    scratch = tmp_path / 'scratch'

    with pytest.raises(ValueError):
        DBCache(str(data), str(staging), str(scratch))

    data.mkdir()
    db = DBCache(str(data), str(staging), str(scratch))
    try:
        assert staging.is_dir()
        assert (scratch / 'ingester-export').is_dir()
    finally:
        db.close()
