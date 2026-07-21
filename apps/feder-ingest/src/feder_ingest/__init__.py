from datetime import datetime, timezone
import logging
import os
from pathlib import Path
from queue import PriorityQueue
import re
import shutil
import signal
import time

import click
from prometheus_client import start_http_server

from feder_server import (
    logging_setup, Config, FILE_ONLY_CONFIG_REQUIREMENTS,
    FILE_QUEUE_INGEST_CONFIG_REQUIREMENTS, INGEST_CONFIG_REQUIREMENTS,
    rmq_parameters, RMQ_TRAJECTORY_EXCHANGE, validate_path_roots,
    IngesterLivenessChecker, Message, TrajectoryBatch,
    error_counter, set_version, TimerThread
)
from feder_server.netcdf import read_trajectory_batch_netcdf
from feder_server.rmq import RMQ, Consumer

from .commands import RMQCommand, CheckpointCommand
from .db_cache import DBCache
from .processor import Processor


__version__ = '1.2.1'


logger = logging.getLogger(__name__)


# This just forces the database cache to commit all its open databases and
# write in-memory databases to disk every 15 minutes. It's only really needed
# in quiet periods when no new trajectories are coming in.

class CheckpointTimerThread(TimerThread):
    def __init__(self, queue: PriorityQueue):
        super().__init__(queue, 15 * 60, CheckpointCommand)


@click.command()
@click.option(
    '--debug/--no-debug', default=False,
    help='Set logging level to DEBUG.'
)
@click.option(
    '--config', '-c',
    help='Path to Feder configuration file'
)
@click.option(
    '--file-input-directory',
    type=click.Path(path_type=str),
    help='Process visible *.nc trajectory-batch files from this directory and exit.'
)
@click.option(
    '--file-input-queue',
    is_flag=True,
    help='Drain scheduled receiver ready runs and exit.'
)
def run(
        debug: bool, config: str | None, file_input_directory: str | None,
        file_input_queue: bool,
) -> None:
    logging_setup(debug)

    if file_input_directory is not None and file_input_queue:
        raise click.UsageError(
            '--file-input-queue is mutually exclusive with --file-input-directory'
        )

    # Finite file modes are intentionally file-only: they must not require or
    # construct RabbitMQ/Prometheus objects. Queue mode additionally requires
    # the receiver-owned queue root in order to derive its ready directory.
    if file_input_queue:
        requirements = FILE_QUEUE_INGEST_CONFIG_REQUIREMENTS
    elif file_input_directory is not None:
        requirements = FILE_ONLY_CONFIG_REQUIREMENTS
    else:
        requirements = INGEST_CONFIG_REQUIREMENTS
    cfg = Config(config, requirements=requirements)

    if file_input_queue:
        _run_file_input_queue_mode(cfg)
        return
    if file_input_directory is not None:
        _run_file_input_mode(cfg, file_input_directory)
        return

    # Start Prometheus HTTP server.
    prom_server, prom_thread = start_http_server(cfg.ingester_prometheus_port)

    # Set version information for Prometheus..
    set_version()

    # Set up command queue.
    queue = PriorityQueue(10)

    name = 'ingester'

    # For exception handling...
    db = None
    rmq = None
    processor = None
    checkpoint_timer = None

    # Signal handling for tidy cleanup.
    def stop(_signum, _frame):
        if processor is None:
            return
        processor.immediate_stop()
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    # NOTE: Any errors that occur up to this point are considered start-up
    # errors and should be diagnosed via logging. All errors after this point
    # cause an increment in the Prometheus error_counter metric so that they
    # can be captured by monitoring alerts.
    error_counter.labels(source='ingester').inc(0)

    clean_stop = False
    last_exception = None
    exception_backoff = 5  # seconds
    while not clean_stop:
        try:
            # Set up RabbitMQ handler.
            rmq = RMQ(
                name=name,
                parameters=rmq_parameters(cfg),
                out_queue=queue,
                message_class=Message,
                exchanges=[RMQ_TRAJECTORY_EXCHANGE],
                consumers=[
                    Consumer(RMQ_TRAJECTORY_EXCHANGE, TrajectoryBatch, durable=True)
                ],
                wrapper_class=RMQCommand,
                rpc_client=True,
                rpc_server=[IngesterLivenessChecker.RPC_ENDPOINT_NAME],
                rpc_endpoints=IngesterLivenessChecker.RPC_ENDPOINTS
            )

            # Connect to RabbitMQ.
            rmq.start()

            # Set up database connection cache.
            db = DBCache(
                cfg.data_directory,
                cfg.staging_directory,
                cfg.scratch_directory,
                export_interval=cfg.ingester_export_interval,
                finalize_after=cfg.ingester_finalize_after,
            )

            # Set up checkpoint timer thread.
            checkpoint_timer = CheckpointTimerThread(queue)

            # Start checkpoint timer thread.
            checkpoint_timer.start()

            # Process messages from queue.
            processor = Processor(cfg, db, queue, rmq)
            processor.run()

            # If we get here without an exception, the ingester has stopped
            # cleanly.
            clean_stop = True
        except Exception:
            logger.exception('unhandled exception in ingester')
            error_counter.labels(source='ingester').inc()
            this_exception = datetime.now()
            if last_exception is not None:
                delta = (this_exception - last_exception).total_seconds()
                if delta > exception_backoff:
                    exception_backoff = 5
                else:
                    logger.warning(
                        'repeated exception in ingester: persistent error?'
                    )
                    exception_backoff = min(300, exception_backoff * 2)
            last_exception = this_exception
            time.sleep(exception_backoff)
            last_exception = datetime.now()
            logger.info('restarting ingester after unhandled exception')
        finally:
            if checkpoint_timer is not None:
                checkpoint_timer.stop()
                checkpoint_timer = None
            if db is not None:
                db.close()
                db = None

            # If we get here, the ingester has already stopped, so we just need to
            # clean up RabbitMQ.
            if rmq is not None:
                rmq.stop()
                rmq = None

            # Drain the command queue to prevent any threads that want to
            # write to it getting stuck.
            while not queue.empty():
                queue.get()

    # Shut down Prometheus HTTP server.
    prom_server.shutdown()
    prom_thread.join()


def _validate_file_input_directory(path: str, cfg: Config) -> str:
    input_dir = os.path.abspath(os.path.expanduser(path))
    if not os.path.exists(input_dir):
        raise click.ClickException(
            f'file input directory does not exist: {path}'
        )
    if not os.path.isdir(input_dir):
        raise click.ClickException(
            f'file input path is not a directory: {path}'
        )

    try:
        validate_path_roots({
            'file-input-directory': input_dir,
            'paths/data-directory': cfg.data_directory,
            'paths/staging-directory': cfg.staging_directory,
            'paths/scratch-directory': cfg.scratch_directory,
        })
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
    return input_dir


def _run_file_input_mode(cfg: Config, file_input_directory: str) -> None:
    input_dir = _validate_file_input_directory(file_input_directory, cfg)
    db = DBCache(
        cfg.data_directory,
        cfg.staging_directory,
        cfg.scratch_directory,
        export_interval=cfg.ingester_export_interval,
        finalize_after=cfg.ingester_finalize_after,
    )
    processor = Processor(cfg, db, PriorityQueue(), None)

    try:
        for entry in sorted(os.scandir(input_dir), key=lambda e: e.name):
            if entry.name.startswith('.'):
                continue
            if not entry.is_file() or not entry.name.endswith('.nc'):
                logger.warning('ignoring non-NetCDF input entry: %s', entry.path)
                continue

            try:
                batch = read_trajectory_batch_netcdf(entry.path)
            except Exception as exc:
                raise click.ClickException(
                    f'failed to read NetCDF trajectory batch {entry.path}: {exc}'
                ) from exc

            processor.process_trajectory_batch(batch)
            os.remove(entry.path)
            logger.info('processed and removed NetCDF input file: %s', entry.path)

        db.force_publish()
    except Exception:
        error_counter.labels(source='ingester').inc()
        raise
    finally:
        db.close()


_RUN_DIRECTORY_RE = re.compile(
    r'^contrails-api-(?P<start>\d{8}T\d{6}Z)-'
    r'(?P<end>\d{8}T\d{6}Z)-(?P<run_id>[0-9a-f]{32})$'
)


def _parse_ready_run(entry: os.DirEntry[str]) -> tuple[datetime, datetime, str]:
    """Validate and return the scheduled receiver interval for a ready run."""
    match = _RUN_DIRECTORY_RE.fullmatch(entry.name)
    if match is None or not entry.is_dir(follow_symlinks=False):
        raise click.ClickException(f'unexpected ready queue entry: {entry.path}')
    try:
        start = datetime.strptime(match['start'], '%Y%m%dT%H%M%SZ').replace(
            tzinfo=timezone.utc
        )
        end = datetime.strptime(match['end'], '%Y%m%dT%H%M%SZ').replace(
            tzinfo=timezone.utc
        )
    except ValueError as exc:
        raise click.ClickException(f'invalid scheduled run name: {entry.name}') from exc
    if (
            start.minute or start.second or start.microsecond or
            end.minute or end.second or end.microsecond or end <= start
    ):
        raise click.ClickException(f'invalid scheduled run interval: {entry.name}')
    return start, end, entry.path


def _ready_queue_directory(cfg: Config) -> str:
    assert cfg.receiver_queue_directory is not None
    ready = Path(cfg.receiver_queue_directory).expanduser() / 'ready'
    if not ready.exists():
        raise click.ClickException(f'receiver ready directory does not exist: {ready}')
    if not ready.is_dir():
        raise click.ClickException(f'receiver ready path is not a directory: {ready}')
    return str(ready)


def _validated_run_files(run_path: str) -> list[str]:
    files = []
    for entry in os.scandir(run_path):
        if entry.name.startswith('.'):
            continue
        if not entry.is_file(follow_symlinks=False) or not entry.name.endswith('.nc'):
            raise click.ClickException(f'unexpected ready run entry: {entry.path}')
        files.append(entry.path)
    return sorted(files)


def _remove_ready_run(run_path: str) -> None:
    # Visible entries were validated before publication. Hidden entries belong
    # to the receiver's private temporary protocol and are removed only after
    # the publication barrier along with the now-complete run directory.
    shutil.rmtree(run_path)


def _run_file_input_queue_mode(cfg: Config) -> None:
    """Drain a fixed snapshot of receiver ready runs, one durable commit at a time."""
    ready_dir = _ready_queue_directory(cfg)
    # Snapshot and validate before mutating any database or input. Hidden
    # receiver temporary entries are intentionally outside the handoff contract.
    runs = [
        _parse_ready_run(entry)
        for entry in os.scandir(ready_dir)
        if not entry.name.startswith('.')
    ]
    runs.sort(key=lambda run: (run[0], run[1], run[2]))

    db = DBCache(
        cfg.data_directory,
        cfg.staging_directory,
        cfg.scratch_directory,
        export_interval=cfg.ingester_export_interval,
        finalize_after=cfg.ingester_finalize_after,
    )
    processor = Processor(cfg, db, PriorityQueue(), None, strict_inserts=True)
    try:
        for _start, _end, run_path in runs:
            files = _validated_run_files(run_path)
            for path in files:
                try:
                    batch = read_trajectory_batch_netcdf(path)
                except Exception as exc:
                    raise click.ClickException(
                        f'failed to read NetCDF trajectory batch {path}: {exc}'
                    ) from exc
                processor.process_trajectory_batch(batch)

            # This is the commit barrier: input stays durable until every
            # batch in the run is published as a public snapshot.
            db.force_publish()
            _remove_ready_run(run_path)
            logger.info('published and removed ready receiver run: %s', run_path)

        # A queue can be empty while staging remains dirty from an earlier
        # failed publish; always retry it before this finite invocation exits.
        if not runs:
            db.force_publish()
    except Exception:
        error_counter.labels(source='ingester').inc()
        raise
    finally:
        db.close()


if __name__ == '__main__':
    run()
