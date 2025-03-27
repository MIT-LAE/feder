import logging
from queue import Queue
import signal
import sys

import click

from feder.server import Config, logging_setup

from .commands import (
    SourcePositionCommand, SourceErrorCommand, SourceDoneCommand,
    HeartbeatCommand,
    CompleteCommand, TrajectoryCommand, CleanCommand,
    StopCommand
)
from .sources.contrails_api import ContrailsAPISource
from .sources.csv import CSVSource
from .sources.flightaware import FlightAwareSource
from .sources.opensky import OpenSkySource, OpenSkyStateVectorSource
from .timers import HeartbeatTimerThread, CompletionTimerThread
import feder.rx.staging as staging


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

    # We may sometimes want to purge the staging database before starting
    # (mostly for debugging).
    if purge_staging:
        staging.purge(cfg, source)

    # Connect to RabbitMQ.
    # TODO: RabbitMQ connection.

    # Connect to staging database for current source.
    db = staging.open(cfg, source)

    # Set up the command queue used to decouple the data source handler and
    # trajectory completion and RabbitMQ connection handling. We need to
    # handle all these things in parallel. For the CSV source, access to the
    # "source" is synchronous and we control the rate at which data is sent to
    # the ingester, but for the other sources, we have less control over the
    # rate that data comes in so we need some mechanism to decouple the source
    # handling from the communication with the ingester via RabbitMQ. We use a
    # queue to do this.
    queue = Queue()

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

    # Process messages from command queue.
    done = False
    while not done:
        match queue.get():
            case SourcePositionCommand():
                print('SOURCE-POSITION command')
            case SourceErrorCommand():
                print('SOURCE-ERROR command')
            case SourceDoneCommand():
                print('SOURCE-DONE command')
                # TODO: Make this run a final trajectory completion cycle.
            case StopCommand():
                done = True
            case HeartbeatCommand():
                print('HEARTBEAT command')
            case CompleteCommand():
                print('COMPLETE command')
            case TrajectoryCommand():
                print('TRAJECTORY command')
            case CleanCommand():
                print('CLEAN command')

    heartbeat_timer_thread.stop()
    completion_timer_thread.stop()


if __name__ == '__main__':
    run()
