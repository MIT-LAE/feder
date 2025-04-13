from queue import Queue
import queue
from threading import Thread, Event
import time
from typing import cast

from pika import ConnectionParameters
import pytest

from feder.server.rmq import (
    RMQ, Consumer, RPCEndpoint, DataMessage, RPCMessage
)

from .test_pb2 import (  # noqa
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


def fib(n):
    if n < 2:
        return 1
    else:
        return fib(n - 1) + fib(n - 2)


def fact(n):
    if n < 2:
        return 1
    else:
        return n * fact(n - 1)


def test_rpc(rmq_rpc_client, rmq_rpc_server):
    callback_complete = Event()
    callbacks_ok = {}
    saved_correlation_ids = {}

    def result_callback(correlation_id, response):
        if correlation_id not in saved_correlation_ids:
            return

        name, result = saved_correlation_ids[correlation_id]
        # print(f'{correlation_id} ({type(response)})  Name: {response.name} ?= {name}  Result: {response.data} ?= {result}  Success: {response.success}')
        callbacks_ok[name] = (
            response.name == name and
            response.data == result and
            response.success
        )
        del saved_correlation_ids[correlation_id]
        if len(saved_correlation_ids) == 0:
            callback_complete.set()

    def error_callback(correlation_id, error):
        ...

    def client():
        while True:
            qmsg = rmq_rpc_client.out_queue.get()
            # print(f'CLIENT: {qmsg}')
            if qmsg == 'STOP':
                return

    def server():
        while True:
            msg = rmq_rpc_server.out_queue.get()
            # print(f'SERVER: {msg}')
            match msg:
                case 'STOP':
                    return
                case RPCMessage():
                    # print(f'RPC: {msg.endpoint} => {msg.message} ({type(msg.message)})')
                    match msg.endpoint:
                        case 'fibonacci':
                            request = cast(FibonacciRequest, msg.message)
                            # print(f'FibonacciRequest: {request.name} {request.data}')
                            response = FibonacciResponse()
                            response.name = request.name
                            response.data = fib(request.data)
                            response.success = True
                            # print(f'FIB RESPONDING {request.name}: {msg.correlation_id} => {response.data}')
                            rmq_rpc_server.rpc_reply(msg, response)
                        case 'factorial':
                            request = cast(FactorialRequest, msg.message)
                            # print(f'FactorialRequest: {request.name} {request.data}')
                            response = FactorialResponse()
                            response.name = request.name
                            response.data = fact(request.data)
                            response.success = True
                            # print(f'FACT RESPONDING {request.name}: {msg.correlation_id} => {response.data}')
                            rmq_rpc_server.rpc_reply(msg, response)
                        case other:
                            print(f'UKNOWN REQUEST TYPE: {other}')

    server_thread = Thread(target=server)
    server_thread.start()
    client_thread = Thread(target=client)
    client_thread.start()

    # Make some RPC calls.
    for n in range(1, 10):
        if n % 2 == 0:
            request_class = FibonacciRequest
            endpoint = 'fibonacci'
            fn = fib
        else:
            request_class = FactorialRequest
            endpoint = 'factorial'
            fn = fact
        request = request_class()
        name = f'{endpoint}-{n}'
        request.name = name
        request.data = n
        correlation_id = rmq_rpc_client.send_rpc(
            endpoint, request, result_callback
            # , error_callback, timeout=5
        )
        saved_correlation_ids[correlation_id] = (name, fn(n))

    callback_complete.wait(timeout=5)
    rmq_rpc_server.out_queue.put('STOP')
    rmq_rpc_client.out_queue.put('STOP')
    server_thread.join()
    client_thread.join()

    assert len(callbacks_ok) == 9 and all(callbacks_ok.values())


def test_connection_handling(rmq_publisher):
    assert rmq_publisher._connection is not None
    assert rmq_publisher._connection.is_open

    rmq_publisher.stop()
    assert rmq_publisher._connection is None or not rmq_publisher._connection.is_open


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
        if m is not None and isinstance(m, DataMessage)
    ]
    return valid[-1] if len(valid) > 0 else None
