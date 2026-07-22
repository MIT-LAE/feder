from datetime import datetime, timezone
import json
from pathlib import Path
from types import SimpleNamespace

from click.testing import CliRunner
from pandas import Timedelta
import pytest

from feder_common import DataSource
from feder_rx.scheduled import (
    CONTRAILS_SOURCE,
    availability_cutoff,
    load_cursor,
    run_scheduled,
    save_cursor,
    scheduled_run,
)


def _cfg(tmp_path: Path, duration: str = '24 hours'):
    return SimpleNamespace(
        receiver_queue_directory=str(tmp_path / 'queue'),
        receiver_max_run_duration=Timedelta(duration),
        data_lag=lambda source: Timedelta('2 hours') if source == DataSource.CONTRAILS_API else None,
    )


def _time(value: str) -> datetime:
    return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)


def test_bootstrap_persists_before_receiver_and_bounds_interval(tmp_path):
    cfg = _cfg(tmp_path)
    calls = []

    def receiver(start, end, output):
        state = json.loads((tmp_path / 'queue' / 'cursor.json').read_text())
        assert state['next_time'] == '2025-04-01T00:00:00+00:00'
        calls.append((start, end, output))

    assert run_scheduled(cfg, CONTRAILS_SOURCE, '2025-04-01T00:00:00', receiver, _time('2025-04-03T05:34:00'))
    start, end, output = calls[0]
    assert (start, end) == (_time('2025-04-01T00:00:00'), _time('2025-04-02T00:00:00'))
    assert output.parent.name == 'incomplete'
    assert not output.exists()
    ready = list((tmp_path / 'queue' / 'ready').iterdir())
    assert len(ready) == 1
    assert ready[0].name.startswith('contrails-api-20250401T000000Z-20250402T000000Z-')
    assert json.loads((tmp_path / 'queue' / 'cursor.json').read_text())['next_time'] == '2025-04-02T00:00:00+00:00'


def test_cursor_validation_and_no_work_do_not_reinitialize(tmp_path, caplog):
    cfg = _cfg(tmp_path)
    queue = tmp_path / 'queue'
    queue.mkdir()
    cursor = queue / 'cursor.json'
    cursor.write_text('{not json', encoding='utf-8')
    with pytest.raises(ValueError, match='malformed'):
        run_scheduled(cfg, CONTRAILS_SOURCE, '2025-04-01T00:00:00+00:00', lambda *_: None, _time('2025-04-01T04:00:00'))
    assert cursor.read_text(encoding='utf-8') == '{not json'

    save_cursor(cursor, CONTRAILS_SOURCE, _time('2025-04-01T02:00:00'))
    assert not run_scheduled(cfg, CONTRAILS_SOURCE, None, lambda *_: pytest.fail('must not run'), _time('2025-04-01T04:50:00'))
    save_cursor(cursor, CONTRAILS_SOURCE, _time('2025-04-01T03:00:00'))
    assert not run_scheduled(cfg, CONTRAILS_SOURCE, None, lambda *_: pytest.fail('must not run'), _time('2025-04-01T04:50:00'))
    assert 'ahead of cutoff' in caplog.text


def test_failure_removes_handled_incomplete_and_never_advances_cursor(tmp_path):
    cfg = _cfg(tmp_path)
    abandoned = tmp_path / 'queue' / 'incomplete' / 'abandoned-run'
    abandoned.mkdir(parents=True)

    def failure(_start, _end, output):
        (output / 'partial.nc').write_text('partial', encoding='utf-8')
        raise RuntimeError('download failed')

    with pytest.raises(RuntimeError, match='download failed'):
        run_scheduled(cfg, CONTRAILS_SOURCE, '2025-04-01T00:00:00+00:00', failure, _time('2025-04-01T04:00:00'))
    queue = tmp_path / 'queue'
    assert list((queue / 'incomplete').iterdir()) == [abandoned]
    assert list((queue / 'ready').iterdir()) == []
    assert load_cursor(queue / 'cursor.json', CONTRAILS_SOURCE) == _time('2025-04-01T00:00:00')


def test_empty_success_is_published_and_cursor_follows_ready_rename(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    events = []
    original_replace = __import__('feder_rx.scheduled', fromlist=['os']).os.replace

    def record_replace(source, destination):
        events.append((Path(source).name, Path(destination).name))
        return original_replace(source, destination)

    monkeypatch.setattr('feder_rx.scheduled.os.replace', record_replace)
    assert run_scheduled(cfg, CONTRAILS_SOURCE, '2025-04-01T00:00:00+00:00', lambda *_: None, _time('2025-04-01T03:00:00'))
    ready_name = next((tmp_path / 'queue' / 'ready').iterdir()).name
    assert events[-2] == (ready_name, ready_name)
    assert events[-1][1] == 'cursor.json'
    assert list((tmp_path / 'queue' / 'ready' / ready_name).glob('*.nc')) == []


def test_cutoff_flooring_source_and_cli_errors(tmp_path, monkeypatch):
    assert availability_cutoff(_time('2025-04-01T04:59:59') , Timedelta('2 hours').to_pytimedelta()) == _time('2025-04-01T02:00:00')
    with pytest.raises(ValueError, match='unsupported'):
        run_scheduled(_cfg(tmp_path), 'opensky', None, lambda *_: None)
    result = CliRunner().invoke(scheduled_run, ['opensky'])
    assert result.exit_code != 0
    assert 'unsupported scheduled source' in result.output

    for name in ('data', 'staging', 'scratch'):
        (tmp_path / name).mkdir()
    config = tmp_path / 'scheduled.toml'
    config.write_text(f'''\
[paths]
data-directory = "{tmp_path / 'data'}"
staging-directory = "{tmp_path / 'staging'}"
scratch-directory = "{tmp_path / 'scratch'}"

[receiver]
queue-directory = "{tmp_path / 'scheduled-queue'}"
''', encoding='utf-8')
    calls = []
    monkeypatch.setattr(
        'feder_rx.scheduled._execute_file_receiver',
        lambda _config, start, end, output: calls.append((start, end, output)),
    )
    result = CliRunner().invoke(scheduled_run, [
        '--config', str(config), '--initial-start-time', '2025-04-01T00:00:00', CONTRAILS_SOURCE,
    ])
    assert result.exit_code == 0, result.output
    assert len(calls) == 1


def test_cursor_write_is_atomic_and_initial_time_is_required(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    with pytest.raises(ValueError, match='initial-start-time is required'):
        run_scheduled(cfg, CONTRAILS_SOURCE, None, lambda *_: None, _time('2025-04-01T04:00:00'))
    with pytest.raises(ValueError, match='whole UTC hour'):
        run_scheduled(
            cfg, CONTRAILS_SOURCE, '2025-04-01T00:30', lambda *_: None,
            _time('2025-04-01T04:00:00'),
        )

    calls = []
    original_replace = __import__('feder_rx.scheduled', fromlist=['os']).os.replace
    monkeypatch.setattr('feder_rx.scheduled.os.replace', lambda source, dest: calls.append((Path(source), Path(dest))) or original_replace(source, dest))
    save_cursor(tmp_path / 'cursor.json', CONTRAILS_SOURCE, _time('2025-04-01T00:00:00'))
    assert calls[0][0].name.startswith('.cursor.json.')
    assert calls[0][1].name == 'cursor.json'
