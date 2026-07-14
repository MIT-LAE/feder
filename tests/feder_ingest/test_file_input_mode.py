from datetime import datetime, timezone
import sqlite3

from click.testing import CliRunner

from feder_common import DataSource, Point, Trajectory
from feder_ingest import run
from feder_ingest.db_cache import DBCache
from feder_ingest.writeable_db import WritableDB
from feder_server import Trajectory as MessageTrajectory, TrajectoryBatch
from feder_server.netcdf import write_trajectory_batch_netcdf


def _config_text(data, staging, scratch):
    return f'''
[paths]
data-directory = "{data}"
staging-directory = "{staging}"
scratch-directory = "{scratch}"

[ingester]
'''


def _write_config(tmp_path):
    data = tmp_path / 'data'
    staging = tmp_path / 'staging'
    scratch = tmp_path / 'scratch'
    data.mkdir()
    config = tmp_path / 'config.toml'
    config.write_text(_config_text(data, staging, scratch))
    return config, data, staging, scratch


def _trajectory(source_id='source-1', day=None):
    day = day or datetime(2025, 5, 22, 12, tzinfo=timezone.utc)
    return Trajectory(
        source=DataSource.FLIGHTAWARE,
        source_id=source_id,
        transponder_id='ABCDEF',
        orig='KBOS',
        dest='KJFK',
        callsign='TEST123',
        aircraft_type=None,
        points=[Point(
            time=day,
            lon=-71.0,
            lat=42.0,
            alt=30000,
            alt_gnss=None,
            heading=None,
            on_ground=False,
        )],
    )


def _batch(*source_ids):
    return TrajectoryBatch(
        trajectories=[MessageTrajectory(_trajectory(source_id)) for source_id in source_ids],
        source='receiver-a',
        trajectory_count=len(source_ids),
    )


def _count_rows(path):
    conn = sqlite3.connect(path)
    try:
        return conn.execute('SELECT COUNT(*) FROM trajectories').fetchone()[0]
    finally:
        conn.close()


def test_file_input_happy_path_deletes_inputs_publishes_and_skips_rmq(tmp_path, monkeypatch, caplog):
    config, data, _staging, _scratch = _write_config(tmp_path)
    input_dir = tmp_path / 'input'
    input_dir.mkdir()
    write_trajectory_batch_netcdf(input_dir / '002.nc', _batch('second'))
    write_trajectory_batch_netcdf(input_dir / '001.nc', _batch('first'))
    write_trajectory_batch_netcdf(input_dir / '.hidden.nc', _batch('hidden'))
    (input_dir / 'note.txt').write_text('ignore me')

    def fail_if_rmq_constructed(*_args, **_kwargs):
        raise AssertionError('RMQ must not be constructed in file-input mode')

    def fail_if_prometheus_started(*_args, **_kwargs):
        raise AssertionError('Prometheus server must not be started in file-input mode')

    monkeypatch.setattr('feder_ingest.RMQ', fail_if_rmq_constructed)
    monkeypatch.setattr('feder_ingest.start_http_server', fail_if_prometheus_started)

    result = CliRunner().invoke(run, [
        '--config', str(config),
        '--file-input-directory', str(input_dir),
    ])

    assert result.exit_code == 0, result.output
    assert not (input_dir / '001.nc').exists()
    assert not (input_dir / '002.nc').exists()
    assert (input_dir / '.hidden.nc').exists()
    assert (input_dir / 'note.txt').exists()
    assert 'ignoring non-NetCDF input entry' in caplog.text
    assert _count_rows(data / '2025' / '2025-142.sqlite') == 2


def test_file_input_rejects_missing_non_directory_and_overlapping_input(tmp_path):
    config, data, _staging, _scratch = _write_config(tmp_path)
    runner = CliRunner()

    missing = runner.invoke(run, [
        '--config', str(config),
        '--file-input-directory', str(tmp_path / 'missing'),
    ])
    assert missing.exit_code != 0
    assert 'does not exist' in missing.output

    not_dir = tmp_path / 'not-dir'
    not_dir.write_text('x')
    file_result = runner.invoke(run, [
        '--config', str(config),
        '--file-input-directory', str(not_dir),
    ])
    assert file_result.exit_code != 0
    assert 'not a directory' in file_result.output

    overlap = runner.invoke(run, [
        '--config', str(config),
        '--file-input-directory', str(data),
    ])
    assert overlap.exit_code != 0
    assert 'must be distinct' in overlap.output


def test_invalid_netcdf_is_retained_and_later_files_are_not_processed(tmp_path):
    config, data, _staging, _scratch = _write_config(tmp_path)
    input_dir = tmp_path / 'input'
    input_dir.mkdir()
    (input_dir / '001.nc').write_bytes(b'not a NetCDF file')
    write_trajectory_batch_netcdf(input_dir / '002.nc', _batch('later'))

    result = CliRunner().invoke(run, [
        '--config', str(config),
        '--file-input-directory', str(input_dir),
    ])

    assert result.exit_code != 0
    assert (input_dir / '001.nc').exists()
    assert (input_dir / '002.nc').exists()
    assert not (data / '2025' / '2025-142.sqlite').exists()


def test_valid_file_is_deleted_even_when_individual_insert_fails(tmp_path, monkeypatch, caplog):
    config, data, _staging, _scratch = _write_config(tmp_path)
    input_dir = tmp_path / 'input'
    input_dir.mkdir()
    write_trajectory_batch_netcdf(input_dir / '001.nc', _batch('bad', 'good'))

    original_add = DBCache.add_trajectory

    def fail_one(self, traj):
        if traj.source_id == 'bad':
            raise sqlite3.Error('synthetic insert failure')
        return original_add(self, traj)

    monkeypatch.setattr(DBCache, 'add_trajectory', fail_one)

    result = CliRunner().invoke(run, [
        '--config', str(config),
        '--file-input-directory', str(input_dir),
    ])

    assert result.exit_code == 0, result.output
    assert not (input_dir / '001.nc').exists()
    assert 'Database insert failed for ID: bad' in caplog.text
    assert _count_rows(data / '2025' / '2025-142.sqlite') == 1


def test_empty_input_still_retries_dirty_staging_publish(tmp_path):
    config, data, staging, _scratch = _write_config(tmp_path)
    input_dir = tmp_path / 'input'
    input_dir.mkdir()
    db = WritableDB(str(staging), datetime(2025, 5, 22, tzinfo=timezone.utc))
    try:
        db.add_trajectory(_trajectory('staged'))
    finally:
        db.close()

    result = CliRunner().invoke(run, [
        '--config', str(config),
        '--file-input-directory', str(input_dir),
    ])

    assert result.exit_code == 0, result.output
    assert _count_rows(data / '2025' / '2025-142.sqlite') == 1


def test_final_publish_failure_leaves_staging_retryable_for_empty_later_run(tmp_path, monkeypatch):
    config, data, staging, _scratch = _write_config(tmp_path)
    input_dir = tmp_path / 'input'
    input_dir.mkdir()
    write_trajectory_batch_netcdf(input_dir / '001.nc', _batch('needs-retry'))

    fail_publish = True
    original_publish = DBCache._publish_snapshot

    def maybe_fail_publish(self, snapshot_path, ref_date):
        if fail_publish:
            raise RuntimeError('synthetic publish failure')
        return original_publish(self, snapshot_path, ref_date)

    monkeypatch.setattr(DBCache, '_publish_snapshot', maybe_fail_publish)

    first = CliRunner().invoke(run, [
        '--config', str(config),
        '--file-input-directory', str(input_dir),
    ])

    assert first.exit_code != 0
    assert not (input_dir / '001.nc').exists()
    assert (staging / '2025' / '2025-142.sqlite').exists()
    assert not (data / '2025' / '2025-142.sqlite').exists()

    fail_publish = False
    second = CliRunner().invoke(run, [
        '--config', str(config),
        '--file-input-directory', str(input_dir),
    ])

    assert second.exit_code == 0, second.output
    assert _count_rows(data / '2025' / '2025-142.sqlite') == 1
