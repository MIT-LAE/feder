from datetime import datetime, timezone
import sqlite3

import pytest
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


def _write_config(tmp_path, queue=False):
    data = tmp_path / 'data'
    staging = tmp_path / 'staging'
    scratch = tmp_path / 'scratch'
    data.mkdir()
    config = tmp_path / 'config.toml'
    queue_config = ''
    queue_root = None
    if queue:
        queue_root = tmp_path / 'receiver-queue'
        (queue_root / 'ready').mkdir(parents=True)
        queue_config = f'\n[receiver]\nqueue-directory = "{queue_root}"\n'
    config.write_text(_config_text(data, staging, scratch) + queue_config)
    return config, data, staging, scratch, queue_root


def _run_name(start, end, suffix='0' * 32):
    return f'contrails-api-{start}-{end}-{suffix}'


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
    config, data, _staging, _scratch, _queue_root = _write_config(tmp_path)
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
    config, data, _staging, _scratch, _queue_root = _write_config(tmp_path)
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
    config, data, _staging, _scratch, _queue_root = _write_config(tmp_path)
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
    config, data, _staging, _scratch, _queue_root = _write_config(tmp_path)
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
    config, data, staging, _scratch, _queue_root = _write_config(tmp_path)
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
    config, data, staging, _scratch, _queue_root = _write_config(tmp_path)
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


def test_file_input_queue_is_file_only_oldest_first_and_removes_after_publish(tmp_path, monkeypatch):
    config, data, _staging, _scratch, queue_root = _write_config(tmp_path, queue=True)
    ready = queue_root / 'ready'
    later = ready / _run_name('20250522T020000Z', '20250522T030000Z', 'b' * 32)
    earlier = ready / _run_name('20250522T000000Z', '20250522T010000Z', 'a' * 32)
    later.mkdir()
    earlier.mkdir()
    write_trajectory_batch_netcdf(later / 'later.nc', _batch('later'))
    write_trajectory_batch_netcdf(earlier / 'earlier.nc', _batch('earlier'))
    events = []
    original_publish = DBCache.force_publish
    import shutil
    original_rmtree = shutil.rmtree

    def publish(self):
        events.append('publish')
        return original_publish(self)

    def remove(path):
        events.append(f'remove:{path.rsplit("/", 1)[-1]}')
        return original_rmtree(path)

    monkeypatch.setattr(DBCache, 'force_publish', publish)
    monkeypatch.setattr('feder_ingest.shutil.rmtree', remove)
    monkeypatch.setattr('feder_ingest.RMQ', lambda *_a, **_kw: pytest.fail('RMQ started'))
    monkeypatch.setattr('feder_ingest.start_http_server', lambda *_a, **_kw: pytest.fail('Prometheus started'))

    result = CliRunner().invoke(run, ['--config', str(config), '--file-input-queue'])

    assert result.exit_code == 0, result.output
    assert not earlier.exists() and not later.exists()
    assert events == [
        'publish', f'remove:{earlier.name}',
        'publish', f'remove:{later.name}',
    ]
    assert _count_rows(data / '2025' / '2025-142.sqlite') == 2


def test_file_input_queue_snapshots_ready_runs_and_processes_oldest_first(tmp_path, monkeypatch):
    config, _data, _staging, _scratch, queue_root = _write_config(tmp_path, queue=True)
    ready = queue_root / 'ready'
    newest = ready / _run_name('20250522T020000Z', '20250522T030000Z', 'c' * 32)
    oldest = ready / _run_name('20250522T000000Z', '20250522T010000Z', 'a' * 32)
    newest.mkdir()
    oldest.mkdir()
    write_trajectory_batch_netcdf(newest / 'newest.nc', _batch('newest'))
    write_trajectory_batch_netcdf(oldest / 'oldest.nc', _batch('oldest'))
    processed = []
    original_read = __import__('feder_ingest').read_trajectory_batch_netcdf

    def read_and_add(path):
        processed.append(path)
        if len(processed) == 1:
            later = ready / _run_name('20250522T040000Z', '20250522T050000Z', 'd' * 32)
            later.mkdir()
            write_trajectory_batch_netcdf(later / 'later.nc', _batch('later'))
        return original_read(path)

    monkeypatch.setattr('feder_ingest.read_trajectory_batch_netcdf', read_and_add)
    result = CliRunner().invoke(run, ['--config', str(config), '--file-input-queue'])

    assert result.exit_code == 0, result.output
    assert [path.split('/')[-2] for path in processed] == [oldest.name, newest.name]
    assert (ready / _run_name('20250522T040000Z', '20250522T050000Z', 'd' * 32)).exists()


def test_file_input_queue_validates_snapshot_and_run_entries(tmp_path):
    config, _data, _staging, _scratch, queue_root = _write_config(tmp_path, queue=True)
    ready = queue_root / 'ready'
    (ready / 'unexpected.txt').write_text('bad')

    result = CliRunner().invoke(run, ['--config', str(config), '--file-input-queue'])
    assert result.exit_code != 0
    assert 'unexpected ready queue entry' in result.output

    (ready / 'unexpected.txt').unlink()
    run_dir = ready / _run_name('20250522T000000Z', '20250522T010000Z')
    run_dir.mkdir()
    (run_dir / 'note.txt').write_text('bad')
    result = CliRunner().invoke(run, ['--config', str(config), '--file-input-queue'])
    assert result.exit_code != 0
    assert run_dir.exists()
    assert 'unexpected ready run entry' in result.output

    (run_dir / 'note.txt').unlink()
    malformed = ready / _run_name('20250522T001234Z', '20250522T021234Z')
    malformed.mkdir()
    result = CliRunner().invoke(run, ['--config', str(config), '--file-input-queue'])
    assert result.exit_code != 0
    assert malformed.exists()
    assert 'invalid scheduled run interval' in result.output


def test_file_input_queue_removes_hidden_run_entries_after_publication(tmp_path):
    config, _data, _staging, _scratch, queue_root = _write_config(tmp_path, queue=True)
    run_dir = queue_root / 'ready' / _run_name('20250522T000000Z', '20250522T010000Z')
    run_dir.mkdir()
    (run_dir / '.receiver-tmp').write_text('temporary')

    result = CliRunner().invoke(run, ['--config', str(config), '--file-input-queue'])
    assert result.exit_code == 0, result.output
    assert not run_dir.exists()


def test_file_input_queue_empty_run_and_empty_queue_publish(tmp_path, monkeypatch):
    config, _data, _staging, _scratch, queue_root = _write_config(tmp_path, queue=True)
    run_dir = queue_root / 'ready' / _run_name('20250522T000000Z', '20250522T010000Z')
    run_dir.mkdir()
    calls = []
    monkeypatch.setattr(DBCache, 'force_publish', lambda self: calls.append('publish'))

    first = CliRunner().invoke(run, ['--config', str(config), '--file-input-queue'])
    assert first.exit_code == 0, first.output
    assert not run_dir.exists()
    assert calls == ['publish']

    second = CliRunner().invoke(run, ['--config', str(config), '--file-input-queue'])
    assert second.exit_code == 0, second.output
    assert calls == ['publish', 'publish']


def test_file_input_queue_publish_or_insert_failure_retains_and_stops(tmp_path, monkeypatch):
    config, _data, _staging, _scratch, queue_root = _write_config(tmp_path, queue=True)
    ready = queue_root / 'ready'
    first = ready / _run_name('20250522T000000Z', '20250522T010000Z', 'a' * 32)
    later = ready / _run_name('20250522T010000Z', '20250522T020000Z', 'b' * 32)
    first.mkdir()
    later.mkdir()
    write_trajectory_batch_netcdf(first / 'first.nc', _batch('first'))
    write_trajectory_batch_netcdf(later / 'later.nc', _batch('later'))
    monkeypatch.setattr(DBCache, 'force_publish', lambda self: (_ for _ in ()).throw(RuntimeError('publish failed')))

    result = CliRunner().invoke(run, ['--config', str(config), '--file-input-queue'])
    assert result.exit_code != 0
    assert first.exists() and later.exists()

    monkeypatch.undo()
    original_add = DBCache.add_trajectory
    def fail_insert(self, trajectory):
        raise sqlite3.Error('insert failed')
    monkeypatch.setattr(DBCache, 'add_trajectory', fail_insert)
    result = CliRunner().invoke(run, ['--config', str(config), '--file-input-queue'])
    assert result.exit_code != 0
    assert first.exists() and later.exists()
    monkeypatch.setattr(DBCache, 'add_trajectory', original_add)


def test_file_input_options_are_mutually_exclusive(tmp_path):
    config, _data, _staging, _scratch, _queue_root = _write_config(tmp_path, queue=True)
    result = CliRunner().invoke(run, [
        '--config', str(config), '--file-input-queue', '--file-input-directory', str(tmp_path),
    ])
    assert result.exit_code != 0
    assert 'mutually exclusive' in result.output
