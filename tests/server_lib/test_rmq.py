import queue
import time

from feder_server.rmq import DataMessage

from .messages import PubTest


def test_publish_message(rmq_publisher):
    message = PubTest(name = 'test-1')
    message_number = rmq_publisher.send('test_exchange', message)
    assert message_number > 0


def test_consume_message(rmq_publisher, rmq_consumer):
    # Publish a new message.
    message = PubTest(name = 'test-2')
    rmq_publisher.send('test_exchange', message)

    # Consume messages.
    latest_data_msg = _latest_data(rmq_consumer.out_queue)
    assert latest_data_msg is not None
    assert latest_data_msg.message.name == 'test-2'


def test_connection_handling(rmq_publisher):
    assert rmq_publisher._connection is not None
    assert rmq_publisher._connection.is_open

    rmq_publisher.stop()
    assert rmq_publisher._connection is None or not rmq_publisher._connection.is_open


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
