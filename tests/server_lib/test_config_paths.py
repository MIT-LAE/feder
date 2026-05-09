from pathlib import Path

import pytest
from pandas import Timedelta

from feder_server import Config


def _config_text(data: Path, staging: Path | None, scratch: Path) -> str:
    staging_line = '' if staging is None else f'staging-directory = "{staging}"\n'
    return f'''
[paths]
data-directory = "{data}"
{staging_line}scratch-directory = "{scratch}"

[rabbitmq]
host = "none"
username = "none"
password = "none"

[mailjet]
api_key = "none"
secret_key = "none"
from_email = "none@example.com"
from_name = "none"
to_email = "none@example.com"
to_name = "none"

[ingester]
prometheus-port = 19001
'''


def test_config_requires_staging_directory(tmp_path):
    with pytest.raises(SystemExit):
        Config(config_text=_config_text(tmp_path / 'data', None, tmp_path / 'scratch'))


def test_config_rejects_nested_path_roots(tmp_path):
    with pytest.raises(SystemExit):
        Config(config_text=_config_text(
            tmp_path / 'data',
            tmp_path / 'data' / 'staging',
            tmp_path / 'scratch',
        ))


def test_config_accepts_distinct_path_roots_and_ingester_defaults(tmp_path):
    cfg = Config(config_text=_config_text(
        tmp_path / 'data',
        tmp_path / 'staging',
        tmp_path / 'scratch',
    ))

    assert cfg.data_directory == str(tmp_path / 'data')
    assert cfg.staging_directory == str(tmp_path / 'staging')
    assert cfg.scratch_directory == str(tmp_path / 'scratch')
    assert cfg.ingester_export_interval == Timedelta('1 hour')
    assert cfg.ingester_finalize_after == Timedelta('12 hours')
