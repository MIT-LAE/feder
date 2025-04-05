import logging
from queue import Queue
import signal

import click

from feder.server import (
    logging_setup, Config, RMQ, rmq_parameters,
    RMQ_TRAJECTORY_EXCHANGE, RMQ_MONITOR_EXCHANGE,
    TimerThread
)
from feder.server.rabbitmq_pb2 import Trajectory


logger = logging.getLogger(__name__)


class HeartbeatTimerThread(TimerThread):
    def __init__(self, cfg: Config, queue: Queue):
        super().__init__(
            queue, cfg.heartbeat_interval.seconds, lambda: 'HEARTBEAT'
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
def run(debug: bool, config: str | None) -> None:
    logging_setup(debug)

    # Process configuration file.
    cfg = Config(config)

    # Set up database connection cache.
    db = DBCache(cfg)

    # Set up command queue.
    queue = Queue()

    # Set up RabbitMQ handler.
    rmq = RMQ(
        'ingester',
        rmq_parameters(cfg),
        queue,
        [RMQ_TRAJECTORY_EXCHANGE, RMQ_MONITOR_EXCHANGE],
        consumers=[
            RMQ.Consumer(RMQ_TRAJECTORY_EXCHANGE, Trajectory, durable=True)
        ]
    )

    # Set up heartbeat timer threads.
    heartbeat_timer_thread = HeartbeatTimerThread(cfg, queue)

    # Start all separate threads.
    rmq.start()
    heartbeat_timer_thread.start()

    # Signal handling for tidy cleanup.
    def stop(_signum, _frame):
        queue.put('STOP')
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    # Process messages from queue.
    processor = Processor(cfg, db, queue, rmq)
    processor.run()

    # If we get here, the ingester has already stopped, so we just need to
    # clean up the timer threads and RabbitMQ.
    heartbeat_timer_thread.stop()
    rmq.stop()

if __name__ == '__main__':
    run()
