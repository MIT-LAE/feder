from datetime import datetime
import logging
import os
from queue import PriorityQueue
import signal
import sys
import threading

import click
from prometheus_client import start_http_server

from feder_server import (
    logging_setup, Config, RMQ, rmq_parameters,
    RMQ_TRAJECTORY_EXCHANGE, Message, IngesterLivenessChecker,
    error_counter, set_version
)

from .commands import IngesterStatusCommand, RMQCommand
from .sources.contrails_api import ContrailsAPISource
from .sources.csv import CSVSource
from .sources.flightaware import FlightAwareSource
from .sources.opensky import OpenSkySource, OpenSkyStateVectorSource
from .processor import Processor
from .db import DB


__version__ = '0.1.10'


logger = logging.getLogger(__name__)


SOURCES = [
    ContrailsAPISource,
    CSVSource,
    FlightAwareSource,
    OpenSkySource,
    OpenSkyStateVectorSource
]

SOURCES_BY_NAME = {s.source_name(): s for s in SOURCES}

MAX_OUTSTANDING_POSITIONS = 500


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
    '--purge-staging/--no-purge-staging', default=False,
    help='Clear staging database before starting.'
)
@click.option(
    '--start-time', '-s',
    help='Start time (ISO 8601) for historical processing'
)
@click.option(
    '--end-time', '-e',
    help='End time (ISO 8601) for historical processing'
)
@click.option(
    '--file-cache',
    help='Path to directory containing downloaded historical files'
)
@click.argument(
    'source-name',
    type=click.Choice([s.source_name() for s in SOURCES]),
    required=True
)
@click.argument('glob_args', nargs=-1)
def run(
        debug: bool, config: str | None,
        purge_staging: bool,
        start_time: str | None, end_time: str | None,
        file_cache: str | None,
        source_name: str, glob_args: tuple[str, ...]
) -> None:
    source = SOURCES_BY_NAME[source_name]

    logging_setup(debug)

    # Process configuration file.
    cfg = Config(config)

    # Are we running in historical mode or live mode? Historical mode means
    # that we need to provide information about the historical period to
    # process. That usually means a start and end timestamp, but for a
    # file-based receiver like the CSV processor, it will be a list of file
    # globs.
    if (start_time is None) != (end_time is None):
        logger.critical(
            'must provide neither or both of "start-time" and "end-time"'
        )
        sys.exit(1)
    if len(glob_args) != 0 and start_time is not None:
        logger.critical(
            'either give start and end times OR list of file globs'
        )
        sys.exit(1)
    start_datetime: datetime | None = None
    end_datetime: datetime | None = None
    if start_time is not None and end_time is not None:
        try:
            start_datetime = datetime.fromisoformat(start_time)
        except ValueError:
            logger.critical('invalid ISO 8601 time for "start-time"')
            sys.exit(1)
        try:
            end_datetime = datetime.fromisoformat(end_time)
        except ValueError:
            logger.critical('invalid ISO 8601 time for "end-time"')
            sys.exit(1)
    if file_cache is not None:
        if not os.path.exists(file_cache):
            logger.critical('provided file cache directory does not exist')
            sys.exit(1)
    historical = len(glob_args) != 0 or start_time is not None

    # For historical processes, make a unique name.
    name = source_name
    if historical:
        name += f'-{os.getpid()}'

    # Start Prometheus HTTP server for non-historical processes.
    prom_server = None
    prom_thread = None
    if not historical:
        prom_port = cfg.prometheus_port(source.SOURCE)
        if prom_port is not None:
            prom_server, prom_thread = start_http_server(prom_port)

            # Set version information for Prometheus..
            set_version()

    # Set up the command queue used to decouple the data source handler and
    # trajectory completion and RabbitMQ connection handling. We need to
    # handle all these things in parallel. For the CSV source, access to the
    # "source" is synchronous and we control the rate at which data is sent to
    # the ingester, but for the other sources, we have less control over the
    # rate that data comes in so we need some mechanism to decouple the source
    # handling from the communication with the ingester via RabbitMQ. We use a
    # queue to do this.
    command_queue = PriorityQueue(5)

    # For exception handling...
    db = None
    rmq = None
    data_source = None
    ingester_liveness = None

    # Connect to staging database for current source. For historical
    # processing jobs, a unique name is used for the staging database, since
    # the database will be completely consumed at the end of the processing
    # job, and since we don't want historical processing to interfere with any
    # live receivers.
    db = DB(cfg, name, historical)

    # We may sometimes want to purge the staging database before starting
    # (mostly for debugging).
    if purge_staging:
        db.purge()

    name=f'rx-{name}'

    # Set up data source handler.
    data_source = source(
        cfg, command_queue,
        start_time=start_datetime, end_time=end_datetime,
        file_cache=file_cache, glob_args=glob_args
    )

    # Signal handling for tidy cleanup.
    processor = None
    def stop(_signum, _frame):
        if processor is None:
            return
        processor.immediate_stop()
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    error_counter.labels(source=name).inc(0)
    clean_stop = False
    while not clean_stop:
        try:
            # Set up RabbitMQ handler.
            rmq = RMQ(
                name=name,
                parameters=rmq_parameters(cfg),
                out_queue=command_queue,
                message_class=Message,
                exchanges=[RMQ_TRAJECTORY_EXCHANGE],
                wrapper_class=RMQCommand,
                rpc_client=True,
                rpc_endpoints=IngesterLivenessChecker.RPC_ENDPOINTS
            )

            # Start RabbitMQ handler (waits for connection to RabbitMQ
            # broker and throws an exception if it takes too long to get
            # set up).
            rmq.start()

            # Set up ingester liveness checking.
            ingester_liveness = IngesterLivenessChecker(
                rmq, 'ingester', command_queue, IngesterStatusCommand
            )

            # Start the ingester liveness checker and wait for the ingester to be
            # available.
            ingester_liveness.start()
            logger.info('waiting for ingester...')
            ingester_liveness.wait()
            logger.info('ingester is alive')
            if ingester_liveness.live:
                # Start the data source.
                data_source.start()

                # Process messages from queue.
                processor = Processor(
                    cfg, data_source.SOURCE, name, historical, db, command_queue,
                    rmq, data_source.control, ingester_liveness.ok_check_interval
                )
                processor.run()

            # If we get here without an exception, the ingester has
            # stopped cleanly.
            clean_stop = True
        except Exception:
            logger.exception('unhandled exception in receiver')
            if prom_server is not None:
                error_counter.labels(source=name).inc()
        finally:
            # If we get here, the processor has stopped, so we need to
            # clean up the worker threads and RabbitMQ.
            if data_source is not None:
                data_source.stop()
            if ingester_liveness is not None:
                ingester_liveness.stop()
                ingester_liveness = None
            if rmq is not None:
                rmq.stop()
                rmq = None

            # Drain the command queue to prevent any threads that want to
            # write to it getting stuck.
            while not command_queue.empty():
                command_queue.get()

        # Shut down Prometheus HTTP server.
        if prom_server is not None:
            prom_server.shutdown()
        if prom_thread is not None:
            prom_thread.join()

        if db is not None:
            cur = db.conn.cursor()
            remaining = [
                t[0] for t in
                cur.execute('SELECT DISTINCT source_id FROM fixes ORDER BY source_id').fetchall()
            ]
            print('# remaining at end =', len(remaining))

        print('THREADS:')
        for t in threading.enumerate():
            print(t)


if __name__ == '__main__':
    run()
