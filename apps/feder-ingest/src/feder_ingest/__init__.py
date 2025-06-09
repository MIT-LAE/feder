import logging
import os
from queue import PriorityQueue
import signal

import click
from prometheus_client import start_http_server

from feder_server import (
    logging_setup, Config, RMQ, rmq_parameters,
    RMQ_TRAJECTORY_EXCHANGE,
    IngesterLivenessChecker, Consumer, Message, TrajectoryBatch,
    error_counter, set_version, TimerThread
)

from .commands import RMQCommand, CheckpointCommand
from .db_cache import DBCache
from .processor import Processor


__version__ = '0.1.10'


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
def run(debug: bool, config: str | None) -> None:
    logging_setup(debug)

    # Process configuration file.
    cfg = Config(config)

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

    clean_stop = False
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
            db = DBCache(cfg.data_directory)

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


if __name__ == '__main__':
    run()
