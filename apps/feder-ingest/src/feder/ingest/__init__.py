import logging
from queue import PriorityQueue
import signal

import click

from feder.server import (
    logging_setup, Config, RMQ, rmq_parameters,
    RMQ_TRAJECTORY_EXCHANGE, RMQ_MONITOR_EXCHANGE,
    LivenessChecker, Consumer, Message, TrajectoryBatch
)

from .commands import RMQCommand
from .db_cache import DBCache
from .processor import Processor


logger = logging.getLogger(__name__)


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

    # Set up command queue.
    queue = PriorityQueue(10)

    # Set up RabbitMQ handler.
    name = 'ingester'
    rmq = RMQ(
        name=name,
        parameters=rmq_parameters(cfg),
        out_queue=queue,
        message_class=Message,
        exchanges=[RMQ_TRAJECTORY_EXCHANGE, RMQ_MONITOR_EXCHANGE],
        consumers=[
            Consumer(RMQ_TRAJECTORY_EXCHANGE, TrajectoryBatch, durable=True)
        ],
        wrapper_class=RMQCommand,
        rpc_client=True,
        rpc_server=[LivenessChecker.endpoint_name(name)],
        rpc_endpoints=LivenessChecker.rpc_endpoints(cfg)
    )

    # Start all separate threads.
    rmq.start()

    # Signal handling for tidy cleanup.
    processor = None
    def stop(_signum, _frame):
        if processor is None:
            return
        processor.immediate_stop()
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    try:
        # Set up database connection cache.
        db = DBCache(cfg.data_directory)

        # Process messages from queue.
        processor = Processor(cfg, db, queue, rmq)
        processor.run()
    finally:
        db.close()

    # If we get here, the ingester has already stopped, so we just need to
    # clean up RabbitMQ.
    rmq.stop()

    # Drain the command queue to prevent any threads that want to write to it
    # getting stuck.
    while not queue.empty():
        queue.get()

if __name__ == '__main__':
    run()
