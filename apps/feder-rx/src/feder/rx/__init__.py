import logging
import os
from queue import Queue
import signal
import sys

import click

from feder.server import (
    logging_setup, Config, RMQ, rmq_parameters,
    RMQ_TRAJECTORY_EXCHANGE, RMQ_MONITOR_EXCHANGE,
    Message, TimerThread, LivenessChecker
)

from .commands import (
    StopCommand, IngesterStatusCommand, CompleteCommand, RMQCommand
)
from .sources.contrails_api import ContrailsAPISource
from .sources.csv import CSVSource
from .sources.flightaware import FlightAwareSource
from .sources.opensky import OpenSkySource, OpenSkyStateVectorSource
from .processor import Processor
from .db import DB


logger = logging.getLogger(__name__)


SOURCES = [
    ContrailsAPISource,
    CSVSource,
    FlightAwareSource,
    OpenSkySource,
    OpenSkyStateVectorSource
]

SOURCES_BY_NAME = {s.name(): s for s in SOURCES}

MAX_OUTSTANDING_POSITIONS = 500


class CompletionTimerThread(TimerThread):
    def __init__(self, cfg: Config, queue: Queue, source: str):
        super().__init__(
            queue, cfg.completion_interval(source).seconds, CompleteCommand
        )


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
    '--keep-historical-staging/--no-keep-historical-staging', default=False,
    help='Keep staging database for historical runs.'
)
@click.argument(
    'source',
    type=click.Choice([s.name() for s in SOURCES]),
    required=True
)
@click.argument('args', nargs=-1)
def run(
        debug: bool, config: str | None,
        purge_staging: bool, keep_historical_staging: bool,
        source: str, args: tuple[str, ...]
) -> None:
    try:
        logging_setup(debug)

        # Process configuration file.
        cfg = Config(config)

        # Are we running in historical mode or live mode? Historical mode means
        # that we need to provide information about the historical period to
        # process. That usually means a start and end timestamp, but for a
        # file-based receiver like the CSV processor, it will be a list of file
        # globs.
        historical = len(args) != 0

        # Check that the source is enabled for live updates (signalled by not
        # passing in any arguments to run as a historical update process).
        # TODO: Might be better not to do this, but use systemd's enable/disable
        # functionality?
        if not historical and not cfg.enabled(source):
            logger.critical('source "%s" not enabled for live updates', source)
            sys.exit(1)

        # For historical processes, make a unique name.
        name = source
        if historical:
            name += f'-{os.getpid()}'

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

        # Set up the command queue used to decouple the data source handler and
        # trajectory completion and RabbitMQ connection handling. We need to
        # handle all these things in parallel. For the CSV source, access to the
        # "source" is synchronous and we control the rate at which data is sent to
        # the ingester, but for the other sources, we have less control over the
        # rate that data comes in so we need some mechanism to decouple the source
        # handling from the communication with the ingester via RabbitMQ. We use a
        # queue to do this.
        command_queue = Queue(5)

        # Set up RabbitMQ handler.
        name=f'rx-{name}'
        rpc_server = []
        if not historical:
            rpc_server = [LivenessChecker.endpoint_name(name)]
        rmq = RMQ(
            name=name,
            parameters=rmq_parameters(cfg),
            out_queue=command_queue,
            message_class=Message,
            exchanges=[RMQ_TRAJECTORY_EXCHANGE, RMQ_MONITOR_EXCHANGE],
            wrapper_class=RMQCommand,
            rpc_client=True,
            rpc_server=rpc_server,
            rpc_endpoints=LivenessChecker.rpc_endpoints(cfg)
        )

        # Set up data source handler.
        data_source = SOURCES_BY_NAME[source](cfg, command_queue, *args)

        # Set up completion timer threads.
        completion_timer_thread = None
        if not historical:
            completion_timer_thread = CompletionTimerThread(
                cfg, command_queue, data_source.SOURCE
            )

        # Set up ingester liveness checking.
        ingester_liveness = LivenessChecker(
            rmq, 'ingester', command_queue, IngesterStatusCommand
        )

        # Start RabbitMQ handler (waits for connection to RabbitMQ broker and
        # throws an exception if it takes too long to get set up).
        # TODO: Think about retrying here. (OR rely on systemd to restart?)
        try:
            rmq.start()
        except RuntimeError as exc:
            logger.critical('failed to connect to RabbitMQ: %s', exc)
            sys.exit(1)

        # Signal handling for tidy cleanup.
        def stop(_signum, _frame):
            ingester_liveness.stop()
            command_queue.put(StopCommand())
        signal.signal(signal.SIGINT, stop)
        signal.signal(signal.SIGTERM, stop)

        # Start the ingester liveness checker and wait for the ingester to be
        # available.
        ingester_liveness.start()
        logger.info('waiting for ingester...')
        ingester_liveness.wait()
        logger.info('ingester_liveness.wait returned')
        if ingester_liveness.live:
            # Start the other threads.
            if completion_timer_thread is not None:
                completion_timer_thread.start()
            data_source.start()

            # Process messages from queue.
            processor = Processor(
                cfg, data_source.SOURCE, historical, db, command_queue,
                rmq, rpc_server[0] if len(rpc_server) > 0 else None
            )
            processor.run()
    except Exception as e:
        logger.exception('fatal exception: %s', e)
    finally:
        # If we get here, the data source handler has already stopped, so we
        # just need to clean up the other worker threads and RabbitMQ.
        data_source.stop()
        if completion_timer_thread is not None:
            completion_timer_thread.stop()
        ingester_liveness.stop()
        rmq.stop()

    if historical and not keep_historical_staging:
        db.remove(force=True)


if __name__ == '__main__':
    run()
