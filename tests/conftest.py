from datetime import datetime, timedelta
import os
from queue import Queue

from pika import ConnectionParameters
import pytest

from feder.server.config import Config
from feder.rx.db import DB
from feder.server.rmq import RMQ, Consumer, RPCEndpoint

from .test_pb2 import (  # noqa
    PubTest,
    FibonacciRequest, FibonacciResponse,
    FactorialRequest, FactorialResponse
)


TEST_CONFIG = """
[paths]
data-directory = "<PLACEHOLDER>"
scratch-directory = "<PLACEHOLDER>"

[rabbitmq]
host = "none"
username = "none"
password = "none"

[monitoring]
heartbeat-interval = 30
from-email = "feder-monitor@mit.edu"
from-name = "Feder Monitoring"
to-email = "ian@skybluetrades.net"
to-name = "Ian Ross"
mail-backend = "dummy" # for development, normally, default = "mailjet"
mailjet-api-key = "<fill in API key>"
mailjet-secret-key = "<fill in secret key>"

[sources]
# Defaults for all sources.
completion-delay = "15 minutes"
completion-interval = 60
data-lag = 0

[source.contrails-api]
enabled = false
data-lag = "48 hours"
api-key = "<fill in API key>"

[source.flightaware]
enabled = false
username = "<fill in username>"
password = "<fill in password>"

[source.opensky]
enabled = false
api-key = "<fill in API key>"

[source.opensky-state-vectors]
enabled = false
api-key = "<fill in API key>"

[source.csv]
enabled = true
"""


@pytest.fixture
def config(tmp_path):
    cfg = Config(config_text=TEST_CONFIG)
    os.makedirs(tmp_path / 'data')
    os.makedirs(tmp_path / 'scratch')
    cfg.data_directory = tmp_path / 'data'
    cfg.scratch_directory = tmp_path / 'scratch'
    return cfg


TEST_NOW = datetime(2025, 4, 1, 12, 0)

# TIMELINE:
#
# -1050  source-0002
# -1020  source-0004
#  -990  source-0002
#  -960  source-0004
#  -930  source-0002
#  -920  source-0004
#  -920  source-0003
#  -900  15-minute deadline
#  -860  source-0003
#  -800  source-0003
#  -125  source-0001
#   -65  source-0001
#    -5  source-0001

def tminus(seconds):
    return TEST_NOW - timedelta(seconds=seconds)

TEST_VALUES = [
    (
        'source-0001', 'ABCDEF', 'DAL1234',
        [
            (tminus(125), 40.1, -94.5, 35000),
            (tminus( 60), 40.2, -94.4, 35000),
            (tminus(  5), 40.3, -9435, 35000),
        ]
    ),
    (
        'source-0002', 'BCDEF0', 'UPS231',
        [
            (tminus(1050), 40.1, -94.5, 35000),
            (tminus( 990), 40.2, -94.4, 35000),
            (tminus( 930), 40.3, -9435, 35000),
        ]
    ),
    (
        'source-0003', 'CDEF01', 'UAL4747',
        [
            (tminus(920), 40.1, -94.5, 35000),
            (tminus(860), 40.2, -94.4, 35000),
            (tminus(800), 40.3, -9435, 35000),
        ]
    ),
    (
        'source-0004', 'DEF012', 'BA1134',
        [
            (tminus(1020), 40.1, -94.5, 35000),
            (tminus( 960), 40.2, -94.4, 35000),
            (tminus( 920), 40.3, -9435, 35000),
        ]
    )
]


@pytest.fixture
def db(config):
    db = DB(config, 'test')

    for (source_id, transponder_id, callsign, points) in TEST_VALUES:
        for (time, lat, lon, alt) in points:
            db.save_position(
                source_id=source_id, transponder_id=transponder_id,
                time=time, callsign=callsign, aircrafttype=None,
                lat=lat, lon=lon, alt=alt, alt_gnss=None, heading=None,
                on_ground=False
            )

    return db


@pytest.fixture
def rmq_publisher():
    """RMQ publisher instance connected to a local RabbitMQ broker."""
    rmq = _make_rmq('test_publisher')
    rmq.start()
    yield rmq
    try:
        rmq.stop()
    except Exception:
        pass


@pytest.fixture
def rmq_consumer():
    """RMQ consumer instance connected to a local RabbitMQ broker."""
    rmq = _make_rmq('test_consumer', consumer=True)
    rmq.start()
    yield rmq
    try:
        rmq.stop()
    except Exception:
        pass


@pytest.fixture
def rmq_rpc_client():
    """RMQ RPC client instance connected to a local RabbitMQ broker."""
    rmq = _make_rmq('test_rpc_client', rpc_client=True)
    rmq.start()
    yield rmq
    try:
        rmq.stop()
    except Exception:
        pass


@pytest.fixture
def rmq_rpc_server():
    """RMQ RPC server instance connected to a local RabbitMQ broker."""
    rmq = _make_rmq('test_rpc_server', rpc_server=True)
    rmq.start()
    yield rmq
    try:
        rmq.stop()
    except Exception:
        pass


def _make_rmq(
        name: str,
        consumer: bool = False,
        rpc_client: bool = False,
        rpc_server: bool = False
):
    consumers = []
    if consumer:
        consumers = [Consumer(exchange='test_exchange', message_class=PubTest)]

    return RMQ(
        name=name,
        parameters=ConnectionParameters(host='localhost'),
        out_queue=Queue(),
        exchanges=['test_exchange'],
        consumers=consumers,
        rpc_client=rpc_client,
        rpc_server=['fibonacci', 'factorial'] if rpc_server else None,
        rpc_endpoints=[
            RPCEndpoint(
                name='fibonacci',
                request_class=FibonacciRequest,
                response_class=FibonacciResponse
            ),
            RPCEndpoint(
                name='factorial',
                request_class=FactorialRequest,
                response_class=FactorialResponse
            )
        ]
    )
