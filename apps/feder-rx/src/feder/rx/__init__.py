import logging
from queue import PriorityQueue
import signal
import sys

import click

from feder.server import Config, logging_setup

from .commands import StopCommand
from .sources.contrails_api import ContrailsAPISource
from .sources.csv import CSVSource
from .sources.flightaware import FlightAwareSource
from .sources.opensky import OpenSkySource, OpenSkyStateVectorSource
from .timers import HeartbeatTimerThread, CompletionTimerThread
from .processor import Processor
from .staging import DB


logger = logging.getLogger(__name__)

SOURCES = [
    ContrailsAPISource,
    CSVSource,
    FlightAwareSource,
    OpenSkySource,
    OpenSkyStateVectorSource
]

SOURCES_BY_NAME = {s.NAME: s for s in SOURCES}


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
@click.argument('files', nargs=-1)
def run(
        debug: bool, config: str | None, purge_staging: bool,
        source: str, files: tuple[str]
) -> None:
    logging_setup(debug)

    # Process configuration file.
    cfg = Config(config)

    # Check that the source is enabled.
    # TODO: Might be better not to do this, but use systemd's enable/disable
    # functionality?
    if not cfg.enabled(source):
        logger.critical('source "%s" not enabled', source)
        sys.exit(1)

    # Connect to staging database for current source.
    db = DB(cfg, source)

    # We may sometimes want to purge the staging database before starting
    # (mostly for debugging).
    if purge_staging:
        db.purge()

    # Connect to RabbitMQ.
    # TODO: RabbitMQ connection.

    # Set up the command queue used to decouple the data source handler and
    # trajectory completion and RabbitMQ connection handling. We need to
    # handle all these things in parallel. For the CSV source, access to the
    # "source" is synchronous and we control the rate at which data is sent to
    # the ingester, but for the other sources, we have less control over the
    # rate that data comes in so we need some mechanism to decouple the source
    # handling from the communication with the ingester via RabbitMQ. We use a
    # queue to do this.
    queue = PriorityQueue()

    # Set up heartbeat and completion timer threads.
    heartbeat_timer_thread = HeartbeatTimerThread(cfg, queue)
    completion_timer_thread = CompletionTimerThread(cfg, queue, source)

    # Set up data source handler.
    # source = SOURCES_BY_NAME[source](cfg, queue, *files)

    # Start all separate threads.
    heartbeat_timer_thread.start()
    completion_timer_thread.start()
    # source.start()

    # Signal handling for tidy cleanup.
    def stop(_signum, _frame):
        queue.put(StopCommand())
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    # Process messages from queue.
    processor = Processor(db, queue)
    processor.run()

    # If we get here, the data source handler has already stopped, so we just
    # need to clean up the timer threads.
    heartbeat_timer_thread.stop()
    completion_timer_thread.stop()


if __name__ == '__main__':
    run()
