from queue import Queue
import queue
from threading import Thread, Event
import time
from typing import cast

from pika import ConnectionParameters
import pytest

from feder.server.rmq import RMQ, Consumer, RPCEndpoint, MessageType, Message

from .test_pb2 import PubTest, RPCRequest, RPCResponse


@pytest.fixture
def out_queue():
    return Queue()


@pytest.fixture
def rmq_publisher(out_queue):
    """RMQ publisher instance connected to a local RabbitMQ broker."""
    rmq = _make_rmq('test_publisher', out_queue)
    rmq.start()
    yield rmq
    try:
        rmq.stop()
    except Exception:
        pass


@pytest.fixture
def rmq_consumer(out_queue):
    """RMQ consumer instance connected to a local RabbitMQ broker."""
    rmq = _make_rmq('test_consumer', out_queue, consumer=True)
    rmq.start()
    yield rmq
    try:
        rmq.stop()
    except Exception:
        pass


@pytest.fixture
def rmq_rpc_client(out_queue):
    """RMQ RPC client instance connected to a local RabbitMQ broker."""
    rmq = _make_rmq('test_rpc_client', out_queue, rpc_client=True)
    rmq.start()
    yield rmq
    try:
        rmq.stop()
    except Exception:
        pass


@pytest.fixture
def rmq_rpc_server(out_queue):
    """RMQ RPC server instance connected to a local RabbitMQ broker."""
    rmq = _make_rmq('test_rpc_server', out_queue, rpc_server=True)
    rmq.start()
    yield rmq
    try:
        rmq.stop()
    except Exception:
        pass


def test_publisher_initialization(rmq_publisher):
    assert rmq_publisher.name == 'test_publisher'
    assert rmq_publisher.exchanges == ['test_exchange']
    assert len(rmq_publisher.consumers) == 0


def test_consumer_initialization(rmq_consumer):
    assert rmq_consumer.name == 'test_consumer'
    assert rmq_consumer.exchanges == ['test_exchange']
    assert len(rmq_consumer.consumers) == 1
    assert rmq_consumer.consumers[0].exchange == 'test_exchange'


def test_rpc_client_initialization(rmq_rpc_client):
    assert rmq_rpc_client.name == 'test_rpc_client'
    assert rmq_rpc_client.exchanges == ['test_exchange', 'rpc']
    assert len(rmq_rpc_client.consumers) == 0


def test_rpc_server_initialization(rmq_rpc_server):
    assert rmq_rpc_server.name == 'test_rpc_server'
    assert rmq_rpc_server.exchanges == ['test_exchange', 'rpc']
    assert len(rmq_rpc_server.consumers) == 0


def test_publish_message(rmq_publisher):
    message = PubTest()
    message.name = 'test-1'
    message_number = rmq_publisher.send('test_exchange', message)
    assert message_number > 0


def test_consume_message(rmq_publisher, rmq_consumer):
    # Publish a new message.
    message = PubTest()
    message.name = 'test-2'
    rmq_publisher.send('test_exchange', message)

    # Consume messages.
    latest_data_msg = _latest_data(rmq_consumer.out_queue)
    assert latest_data_msg is not None
    assert latest_data_msg.message.name == 'test-2'


def test_rpc(rmq_rpc_client, rmq_rpc_server):
    callback_complete = Event()
    callback_ok = False
    saved_correlation_id = None

    def result_callback(correlation_id, response):
        print('CALLBACK CALLED!')
        nonlocal callback_ok

        if correlation_id != saved_correlation_id:
            return

        callback_ok = (
            response.name == 'test-request' and
            response.data == 13 and
            response.success
        )
        callback_complete.set()

    def error_callback(correlation_id, error):
        ...

    def fib(n):
        if n < 2:
            return 1
        else:
            return fib(n - 1) + fib(n - 2)

    def server():
        while True:
            qmsg = rmq_rpc_server.out_queue.get()
            print(f'qmsg = {qmsg}')
            match qmsg:
                case 'STOP':
                    return
                case Message() as msg:
                    if msg.message_type != MessageType.RPC:
                        continue
                    request = cast(RPCRequest, msg.message)
                    response = RPCResponse()
                    response.name = request.name
                    response.data = fib(request.data)
                    response.success = True
                    rmq_rpc_server.rpc_reply(msg, response)

    server_thread = Thread(target=server)
    server_thread.start()

    # Make an RPC call.
    request = RPCRequest()
    request.name = 'test-request'
    request.data = 6
    saved_correlation_id = rmq_rpc_client.send_rpc(
        'fibonacci', request, RPCResponse,
        result_callback, error_callback, timeout=5
    )

    callback_complete.wait(timeout=5)
    rmq_rpc_server.out_queue.put('STOP')
    server_thread.join()

    assert callback_ok


def test_connection_handling(rmq_publisher):
    assert rmq_publisher._connection is not None
    assert rmq_publisher._connection.is_open

    rmq_publisher.stop()
    assert rmq_publisher._connection is None or not rmq_publisher._connection.is_open


def _make_rmq(
        name: str,
        out_queue: Queue,
        consumer: bool = False,
        rpc_client: bool = False,
        rpc_server: bool = False
):
    consumers = []
    if consumer:
        consumers = [Consumer(exchange='test_exchange', message_class=PubTest)]
    rpc_endpoints = []
    if rpc_server:
        rpc_endpoints = [
            RPCEndpoint(
                name='fibonacci',
                request_class=RPCRequest,
                response_class=RPCResponse
            )
        ]

    return RMQ(
        name=name,
        parameters=ConnectionParameters(host='localhost'),
        out_queue=out_queue,
        exchanges=['test_exchange'],
        consumers=consumers,
        rpc_client=rpc_client,
        rpc_server=rpc_endpoints
    )


def _latest_data(q):
    # This is pretty awkward. The problem is that RabbitMQ is totally
    # asynchronous and the broker may send data and ACK/NACK messages at any
    # time. In the real applications we're writing, it's not a problem, since
    # the processes are all long-lived and can just wait for the next message
    # from the broker. Here though, we want to consume all messages from the
    # broker as far as possible. The timeouts used here work reliably for a
    # RabbitMQ broker running on localhost on a Linux development laptop, but
    # they shouldn't be relied on.
    # TODO: Think of a better way of doing this!
    msgs = []
    try:
        while True:
            msgs.append(q.get(timeout=1))
            time.sleep(0.1)
            if  q.empty():
                break
    except queue.Empty:
        pass
    valid = [
        m for m in msgs
        if m is not None and m.message_type == MessageType.DATA
    ]
    return valid[-1] if len(valid) > 0 else None
