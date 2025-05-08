import logging
from queue import PriorityQueue
import signal
import sys

import click

from feder.common import DataSource
from feder.server import (
    logging_setup, Config, RMQ, rmq_parameters,
    Message, RMQ_TRAJECTORY_EXCHANGE,
    LivenessChecker, TimerThread
)

from .commands import TriggerCommand, StatusCommand
from .processor import Processor


logger = logging.getLogger(__name__)


class TriggerTimerThread(TimerThread):
    def __init__(self, cfg: Config, queue: PriorityQueue):
        super().__init__(
            queue, cfg.monitor_send_interval.seconds, TriggerCommand
        )


# - Repeatedly check liveness of ingester and all enabled sources.
# - Liveness responses from sources:
#    - source name
#    - timestamp
#    - # of points in staging DB
#    - # of unique flights in staging DB
#    - time of last point in staging DB
#    - # of trajectories sent to ingester since started
# - Liveness responses from ingester:
#    - timestamp
#    - information about last trajectory times? from each source?
# - Record response times of liveness requests.
# - Also check repeatedly on:
#    - RabbitMQ queue length for ingester (trajectory.ingester)
#    - Other RabbitMQ statistics?
# - Save results to text file?
# - Save multiple snapshots/time series.
# - Make results accessible to MCAST-board as well somehow.
# - Email notifications when things are down for a long time? When no data
#   has been ingested for enabled sources for a long time?

@click.command()
@click.option(
    '--debug/--no-debug', default=False,
    help='Set logging level to DEBUG.'
)
@click.option(
    '--config', '-c',
    help='Path to Feder configuration file'
)
def run(debug: bool, config: str | None):
    logging_setup(debug)

    # Process configuration file.
    cfg = Config(config)

    # Set up command queue.
    command_queue = PriorityQueue(10)

    # Set up RabbitMQ handler.
    name = 'ingester'
    rmq = RMQ(
        name=name,
        parameters=rmq_parameters(cfg),
        out_queue=command_queue,
        message_class=Message,
        exchanges=[RMQ_TRAJECTORY_EXCHANGE],
        rpc_client=True,
        rpc_endpoints=LivenessChecker.rpc_endpoints(cfg)
    )

    trigger_timer_thread = TriggerTimerThread(cfg, command_queue)

    # Set up ingester liveness checking.
    check_interval = int(cfg.monitoring_check_interval.total_seconds())
    status_sources = ['ingester']
    ingester_liveness = LivenessChecker(
        rmq, 'ingester', command_queue, StatusCommand,
        ok_check_interval=check_interval,
        status_extra_kwargs=dict(source='ingester')
    )

    # Set up receiver liveness checking.
    rx_livenesses = []
    for source in DataSource:
        if cfg.enabled(source):
            name = f'rx-{source}'
            rx_livenesses.append(LivenessChecker(
                rmq, name, command_queue, StatusCommand,
                ok_check_interval=check_interval,
                status_extra_kwargs=dict(source=name)
            ))
            status_sources.append(name)

    # Start RabbitMQ handler (waits for connection to RabbitMQ broker and
    # throws an exception if it takes too long to get set up).
    # TODO: Think about retrying here. (OR rely on systemd to restart?)
    try:
        rmq.start()
    except RuntimeError as exc:
        logger.critical('failed to connect to RabbitMQ: %s', exc)
        sys.exit(1)

    # Start all separate threads.
    trigger_timer_thread.start()
    ingester_liveness.start()
    for rx_liveness in rx_livenesses:
        rx_liveness.start()

    # Signal handling for tidy cleanup.
    processor = None
    def stop(_signum, _frame):
        if processor is None:
            return
        processor.immediate_stop()
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    # Process messages from queue.
    processor = Processor(cfg, command_queue, status_sources)
    processor.run()

    # If we get here, the monitor has already stopped, so we just need to
    # clean up RabbitMQ.
    rmq.stop()

    # Drain the command queue to prevent any threads that want to write to it
    # getting stuck.
    while not command_queue.empty():
        command_queue.get()


if __name__ == '__main__':
    run()
