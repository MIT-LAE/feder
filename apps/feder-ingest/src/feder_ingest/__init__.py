import logging
from queue import PriorityQueue
import signal

import click

from feder_server import (
    logging_setup, Config, RMQ, rmq_parameters,
    RMQ_TRAJECTORY_EXCHANGE,
    IngesterLivenessChecker, Consumer, Message, TrajectoryBatch,
    PrometheusServer, error_counter, set_version
)

from .commands import RMQCommand
from .db_cache import DBCache
from .processor import Processor


__version__ = '0.1.5'


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

    # Start Prometheus HTTP server.
    prom_server = PrometheusServer(
        cfg.ingester_prometheus_port, cfg.prometheus_scrape_interval
    )

    # Set version information for Prometheus..
    set_version('ingester')

    # Set up command queue.
    queue = PriorityQueue(10)

    # Set up RabbitMQ handler.
    name = 'ingester'
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

    db = None
    processor = None

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

    try:
        # Connect to RabbitMQ.
        rmq.start()

        # Set up database connection cache.
        db = DBCache(cfg.data_directory)

        # Process messages from queue.
        processor = Processor(cfg, db, queue, rmq)
        processor.run()
    except Exception:
        logger.exception('unhandled exception in ingester')
        error_counter.labels(source='ingester').inc()

        # Ensure that the metric update makes it upstream.
        prom_server.wait_for_scrape()
    finally:
        if db is not None:
            db.close()

        # If we get here, the ingester has already stopped, so we just need to
        # clean up RabbitMQ.
        rmq.stop()

    # Shut down Prometheus HTTP server.
    prom_server.shutdown()

    # Drain the command queue to prevent any threads that want to write to it
    # getting stuck.
    while not queue.empty():
        queue.get()


if __name__ == '__main__':
    run()
