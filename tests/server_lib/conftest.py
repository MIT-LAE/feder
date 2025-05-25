from queue import Queue

from pika import ConnectionParameters
import pytest

from feder_server import RMQ, Consumer, RPCEndpoint

from .messages import (  # noqa
    TestMessage,
    PubTest,
    FibonacciRequest, FibonacciResponse,
    FactorialRequest, FactorialResponse
)


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
        message_class=TestMessage,
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
