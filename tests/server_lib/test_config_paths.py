from pathlib import Path

import pytest
from pandas import Timedelta

from feder_server import (
    Config,
    FILE_ONLY_CONFIG_REQUIREMENTS,
    INGEST_CONFIG_REQUIREMENTS,
    RX_CONFIG_REQUIREMENTS,
    STATE_OF_FEDER_CONFIG_REQUIREMENTS,
)


def _config_text(
        data: Path,
        staging: Path | None,
        scratch: Path,
        *,
        rabbitmq: bool = True,
        mailjet: bool = True,
        ingester_prometheus: bool = True,
) -> str:
    staging_line = '' if staging is None else f'staging-directory = "{staging}"\n'
    rabbitmq_section = '''
[rabbitmq]
host = "none"
username = "none"
password = "none"
''' if rabbitmq else ''
    mailjet_section = '''
[mailjet]
api_key = "none"
secret_key = "none"
from_email = "none@example.com"
from_name = "none"
to_email = "none@example.com"
to_name = "none"
''' if mailjet else ''
    ingester_section = '''
[ingester]
prometheus-port = 19001
''' if ingester_prometheus else '''
[ingester]
'''
    return f'''
[paths]
data-directory = "{data}"
{staging_line}scratch-directory = "{scratch}"
{rabbitmq_section}{mailjet_section}{ingester_section}'''


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


def test_file_only_config_does_not_require_rabbitmq_prometheus_or_mailjet(tmp_path):
    cfg = Config(
        config_text=_config_text(
            tmp_path / 'data',
            tmp_path / 'staging',
            tmp_path / 'scratch',
            rabbitmq=False,
            mailjet=False,
            ingester_prometheus=False,
        ),
        requirements=FILE_ONLY_CONFIG_REQUIREMENTS,
    )

    assert cfg.data_directory == str(tmp_path / 'data')
    assert cfg.staging_directory == str(tmp_path / 'staging')
    assert cfg.scratch_directory == str(tmp_path / 'scratch')
    assert cfg.rabbitmq_host is None
    assert cfg.ingester_prometheus_port is None
    assert cfg.mailjet_api_key is None


def test_rx_config_requires_rabbitmq_but_not_ingester_prometheus_or_mailjet(tmp_path):
    cfg = Config(
        config_text=_config_text(
            tmp_path / 'data',
            tmp_path / 'staging',
            tmp_path / 'scratch',
            mailjet=False,
            ingester_prometheus=False,
        ),
        requirements=RX_CONFIG_REQUIREMENTS,
    )

    assert cfg.rabbitmq_host == 'none'
    assert cfg.ingester_prometheus_port is None
    assert cfg.mailjet_api_key is None


def test_ingest_config_requires_rabbitmq_and_prometheus_but_not_mailjet(tmp_path):
    cfg = Config(
        config_text=_config_text(
            tmp_path / 'data',
            tmp_path / 'staging',
            tmp_path / 'scratch',
            mailjet=False,
        ),
        requirements=INGEST_CONFIG_REQUIREMENTS,
    )

    assert cfg.rabbitmq_host == 'none'
    assert cfg.ingester_prometheus_port == 19001
    assert cfg.mailjet_api_key is None


def test_state_of_feder_config_requires_mailjet_only(tmp_path):
    cfg = Config(
        config_text=_config_text(
            tmp_path / 'data',
            tmp_path / 'staging',
            tmp_path / 'scratch',
            rabbitmq=False,
            ingester_prometheus=False,
        ),
        requirements=STATE_OF_FEDER_CONFIG_REQUIREMENTS,
    )

    assert cfg.rabbitmq_host is None
    assert cfg.ingester_prometheus_port is None
    assert cfg.mailjet_api_key == 'none'


def test_live_rabbitmq_modes_fail_clearly_without_rabbitmq_config(tmp_path):
    with pytest.raises(SystemExit):
        Config(
            config_text=_config_text(
                tmp_path / 'data',
                tmp_path / 'staging',
                tmp_path / 'scratch',
                rabbitmq=False,
                mailjet=False,
            ),
            requirements=INGEST_CONFIG_REQUIREMENTS,
        )
