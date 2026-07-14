from datetime import datetime
import logging
import os
from queue import PriorityQueue
import signal
import time

import click
from prometheus_client import start_http_server

from feder_server import (
    logging_setup, Config, FILE_ONLY_CONFIG_REQUIREMENTS, INGEST_CONFIG_REQUIREMENTS,
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
def run(debug: bool, config: str | None, file_input_directory: str | None) -> None:
    logging_setup(debug)

    # Process configuration file.  Finite file-input mode is intentionally
    # file-only: it must not require or construct RabbitMQ/Prometheus objects.
    requirements = (
        FILE_ONLY_CONFIG_REQUIREMENTS if file_input_directory is not None
        else INGEST_CONFIG_REQUIREMENTS
    )
    cfg = Config(config, requirements=requirements)

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


if __name__ == '__main__':
    run()
