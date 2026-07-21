"""Durable, finite receiver runs for scheduled Contrails retrieval."""

from datetime import datetime, timedelta, timezone
import json
import logging
import os
from pathlib import Path
import shutil
import uuid
from typing import Callable

import click

from feder_server import Config, SCHEDULED_RX_CONFIG_REQUIREMENTS, logging_setup
from feder_common import DataSource


logger = logging.getLogger(__name__)
CURSOR_VERSION = 1
CONTRAILS_SOURCE = 'contrails-api'


def _parse_whole_hour(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f'{field} must be an ISO 8601 UTC whole hour') from exc
    if parsed.tzinfo is None:
        raise ValueError(f'{field} must include a UTC offset')
    parsed = parsed.astimezone(timezone.utc)
    if parsed.minute or parsed.second or parsed.microsecond:
        raise ValueError(f'{field} must be a whole UTC hour')
    return parsed


def _format_time(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def availability_cutoff(now: datetime, data_lag: timedelta) -> datetime:
    """Return the latest exclusive Contrails hour that is expected available."""
    value = (now.astimezone(timezone.utc) - data_lag).replace(
        minute=0, second=0, microsecond=0
    )
    return value


def load_cursor(path: Path, source: str) -> datetime | None:
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
        if not isinstance(data, dict):
            raise ValueError('cursor must be an object')
        if data.get('version') != CURSOR_VERSION:
            raise ValueError(f'unsupported cursor version {data.get("version")!r}')
        if data.get('source') != source:
            raise ValueError('cursor source does not match requested source')
        if not isinstance(data.get('next_time'), str):
            raise ValueError('cursor next_time must be a string')
        return _parse_whole_hour(data['next_time'], 'cursor next_time')
    except (OSError, json.JSONDecodeError, ValueError, TypeError) as exc:
        raise ValueError(f'malformed cursor state at {path}: {exc}') from exc


def save_cursor(path: Path, source: str, next_time: datetime) -> None:
    """Persist cursor state durably before atomically replacing the old state."""
    payload = json.dumps({
        'version': CURSOR_VERSION,
        'source': source,
        'next_time': _format_time(next_time),
    }, sort_keys=True) + '\n'
    temporary = path.with_name(f'.{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp')
    try:
        with open(temporary, 'w', encoding='utf-8') as fp:
            fp.write(payload)
            fp.flush()
            os.fsync(fp.fileno())
        os.replace(temporary, path)
        try:
            directory_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except OSError:
            logger.warning('could not fsync cursor directory %s', path.parent)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _run_directory_name(source: str, start: datetime, end: datetime) -> str:
    def stamp(value: datetime) -> str:
        return value.astimezone(timezone.utc).strftime('%Y%m%dT%H%M%SZ')

    return f'{source}-{stamp(start)}-{stamp(end)}-{uuid.uuid4().hex}'


def run_scheduled(
        cfg: Config,
        source: str,
        initial_start_time: str | None,
        receiver: Callable[[datetime, datetime, Path], None],
        now: datetime | None = None,
) -> bool:
    """Run at most one interval; return whether a run was published."""
    if source != CONTRAILS_SOURCE:
        raise ValueError(f'unsupported scheduled source: {source}')
    assert cfg.receiver_queue_directory is not None
    queue_root = Path(cfg.receiver_queue_directory).expanduser()
    queue_root.mkdir(parents=True, exist_ok=True)
    incomplete = queue_root / 'incomplete'
    ready = queue_root / 'ready'
    incomplete.mkdir(exist_ok=True)
    ready.mkdir(exist_ok=True)

    cursor_path = queue_root / 'cursor.json'
    cursor = load_cursor(cursor_path, source)
    if cursor is None:
        if initial_start_time is None:
            raise ValueError('initial-start-time is required when cursor state is absent')
        cursor = _parse_whole_hour(initial_start_time, 'initial-start-time')
        # Bootstrap is durable before the first external download.
        save_cursor(cursor_path, source, cursor)

    if now is None:
        now = datetime.now(timezone.utc)
    cutoff = availability_cutoff(now, cfg.data_lag(DataSource.CONTRAILS_API).to_pytimedelta())
    if cursor == cutoff:
        logger.info('scheduled receiver has no work: cursor is at cutoff %s', _format_time(cutoff))
        return False
    if cursor > cutoff:
        logger.warning('scheduled receiver cursor %s is ahead of cutoff %s; leaving it unchanged', _format_time(cursor), _format_time(cutoff))
        return False

    end = min(cursor + cfg.receiver_max_run_duration.to_pytimedelta(), cutoff)
    run_dir = incomplete / _run_directory_name(source, cursor, end)
    run_dir.mkdir()
    try:
        receiver(cursor, end, run_dir)
        published = ready / run_dir.name
        os.replace(run_dir, published)
        # This deliberately follows publication: a crash can duplicate a run,
        # but cannot skip an interval.
        save_cursor(cursor_path, source, end)
        logger.info('published scheduled receiver run %s', published)
        return True
    except Exception:
        if run_dir.exists():
            shutil.rmtree(run_dir)
        raise


def _execute_file_receiver(
        config: str | None, start: datetime, end: datetime, output: Path
) -> None:
    # Reuse the established finite file-output CLI path; it deliberately does
    # not construct RabbitMQ or Prometheus services.
    from . import run
    args = [
        '--start-time', _format_time(start),
        '--end-time', _format_time(end),
        '--file-output-directory', str(output),
        CONTRAILS_SOURCE,
    ]
    if config is not None:
        args[0:0] = ['--config', config]
    run.main(args=args, standalone_mode=False)


@click.command(name='feder-rx-scheduled')
@click.option('--debug/--no-debug', default=False, help='Set logging level to DEBUG.')
@click.option('--config', '-c', help='Path to Feder configuration file')
@click.option('--initial-start-time', help='Required UTC whole-hour cursor bootstrap time.')
@click.argument('source')
def scheduled_run(
        debug: bool, config: str | None, initial_start_time: str | None, source: str
) -> None:
    """Publish one cursor-managed scheduled receiver run."""
    logging_setup(debug)
    if source != CONTRAILS_SOURCE:
        raise click.ClickException(f'unsupported scheduled source: {source}')
    cfg = Config(config, requirements=SCHEDULED_RX_CONFIG_REQUIREMENTS)
    try:
        run_scheduled(
            cfg, source, initial_start_time,
            lambda start, end, output: _execute_file_receiver(config, start, end, output),
        )
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
