import logging
import os
from queue import PriorityQueue
import signal
import sys

import click

from feder.server import (
    logging_setup, Config, RMQ, rmq_parameters,
    RMQ_TRAJECTORY_EXCHANGE, RMQ_MONITOR_EXCHANGE,
    TimerThread, LivenessChecker
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

SOURCES_BY_NAME = {s.NAME: s for s in SOURCES}

QUEUE_SIZE = 100  # Command queue size.


# class IngesterCheckTimerThread(TimerThread):
#     def __init__(self, cfg: Config, queue: PriorityQueue):
#         super().__init__(
#             queue, cfg.heartbeat_interval.seconds, CheckIngesterCommand
#         )


class CompletionTimerThread(TimerThread):
    def __init__(self, cfg: Config, queue: PriorityQueue, source: str):
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
@click.argument(
    'source',
    type=click.Choice([s.NAME for s in SOURCES]),
    required=True
)
@click.argument('args', nargs=-1)
def run(
        debug: bool, config: str | None, purge_staging: bool,
        source: str, args: tuple[str, ...]
) -> None:
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
    queue = PriorityQueue(maxsize=QUEUE_SIZE)

    # Set up RabbitMQ handler.
    liveness_endpoint = f'liveness:{name}'
    rmq = RMQ(
        name=f'rx-{name}',
        parameters=rmq_parameters(cfg),
        out_queue=queue,
        exchanges=[RMQ_TRAJECTORY_EXCHANGE, RMQ_MONITOR_EXCHANGE],
        rpc_client=True,
        rpc_server=[liveness_endpoint],
        rpc_endpoints=LivenessChecker.rpc_endpoints(
            liveness_endpoint, 'liveness:ingester'
        )
    )

    # Set up data source handler.
    data_source = SOURCES_BY_NAME[source](cfg, queue, *args)

    # Set up ingester check and completion timer threads.
    # ingester_check_timer_thread = IngesterCheckTimerThread(cfg, queue)
    completion_timer_thread = CompletionTimerThread(cfg, queue, source)

    # Set up ingester liveness checking.
    ingester_liveness = LivenessChecker(rmq, 'feder-ingester')

    # Start all separate threads.
    rmq.start()
    # ingester_check_timer_thread.start()
    completion_timer_thread.start()
    data_source.start()

    # Signal handling for tidy cleanup.
    def stop(_signum, _frame):
        queue.put(StopCommand())
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    # Process messages from queue.
    processor = Processor(cfg, source, historical, db, queue, rmq)
    processor.run()

    # If we get here, the data source handler has already stopped, so we just
    # need to clean up the timer threads and RabbitMQ.
    data_source.stop()
    ingester_check_timer_thread.stop()
    completion_timer_thread.stop()
    rmq.stop()

    if historical:
        db.remove()


if __name__ == '__main__':
    run()
