from dataclasses import dataclass
import functools
import logging
from queue import Queue
from threading import Thread, Event

from google.protobuf.message import Message
from pika import ConnectionParameters, SelectConnection, spec
from pika.exchange_type import ExchangeType
from pika.delivery_mode import DeliveryMode
from pika.exceptions import ChannelClosedByClient

logger = logging.getLogger(__name__)
logging.getLogger('pika').setLevel(logging.WARNING)


class RMQ(Thread):
    """Class to manage RabbitMQ interactions for Feder processes.

       Runs all RabbitMQ interactions in a separate thread using an
       asynchronous connection with publish confirmation. Includes a
       simplified setup mechanism for exchanges, queues and consumers.

       Note:

        - All exchanges are durable, but queues for consumers can be set up as
          durable or not depending on what's needed.
        - Messages can be sent persistent or not as required.
        - Changing the configuration of exchanges or queues may require manual
          resetting via the RabbitMQ management interface.
        - Reconnection logic waits a fixed time to reconnect after an error.
        - Message number count reset happens when the channel is recreated,
          since RabbitMQ message counts are per-channel.
       - It's hard to know when it's OK to stop the IO loop for a clean
         shutdown, because there can be messages in transit. This is left as
         an application-level decision. Access is provided to publish
         confirmation ACK/NACK messages, so it's possible to know if there are
         published messages with outstanding ACKs.

    """
    @dataclass
    class Ack:
        """Publish confirm ACK message passed to output queue."""
        message_number: int

    @dataclass
    class Nack:
        """Publish confirm NACK message passed to output queue."""
        message_number: int

    @dataclass
    class Consumer:
        """Consumer information."""
        exchange: str
        message_class: type
        durable: bool = True

    def __init__(
            self,
            name: str,
            parameters: ConnectionParameters,
            out_queue: Queue,
            exchanges: list[str],
            consumers: list[Consumer] | None = None,
            prefetch_count: int = 1,
            reconnect_interval: float = 5.0,
            ready_wait_interval: float = 5.0,
            *args, **kwargs):
        """Initialise RabbitMQ infrastructure.

        :param str name: Instance name used to make unique RabbitMQ queue
            names
        :param pika.ConnectionParameters parameters: Connection parameters
        :param queue.Queue out_queue: Output queue for consumption messages
            and publish confirmation ACK/NACK messages
        :param list[str] exchanges: List of RabbitMQ exchanges to create (all
            durable)
        :param list[Consumer] | None consumers: Consumption configuration
            associating exchange names with Protocol Buffers message types for
            RabbitMQ message consumption
        :param int prefetch_count: QoS prefetch count
        :param float reconnect_interval: Time to wait (s) to reconnect to
            RabbitMQ after a connection failure
        :param float ready_wait_interval: Time to wait (s) to for RabbitMQ
            infrastructure setup when starting RabbitMQ handler thread

        """
        super().__init__(*args, **kwargs)

        self.name = name
        self.parameters = parameters
        self.out_queue = out_queue
        self.exchanges = exchanges
        self.consumers = consumers
        self.prefetch_count = prefetch_count
        self.reconnect_interval = reconnect_interval
        self.ready_wait_interval = ready_wait_interval

        # We can't do anything without an exchange and we need the requested
        # exchanges to be unique.
        if len(self.exchanges) == 0:
            raise ValueError('no exchange names provided for RMQ')
        if len(set(self.exchanges)) != len(self.exchanges):
            raise ValueError('exchange names for RMQ must be unique')

        # We use RabbitMQ in a stereotyped way where we define one queue for
        # each of the request consuming exchanges, and we assume that all
        # messages coming in from that exchange are instances of a single
        # Protocol Buffers message type.
        if self.consumers is not None:
            if len(set(c.exchange for c in self.consumers)) != len(self.consumers):
                raise ValueError('exchange names must be unique in consumer list')
            for c in self.consumers:
                if c.exchange not in self.exchanges:
                    raise ValueError(
                        f'consumer exchange name "{c.exchange}" not in exchanges list'
                    )
                if not issubclass(c.message_class, Message):
                    raise ValueError(
                        f'receive class "{c.message_class}" not a Protocol Buffers message'
                    )

        # IO loop termination control.
        self._stopping = False

        # Message counter for publish confirmation.
        self._message_number = 0

        # Event to wait for RabbitMQ initialisation.
        self._ready = Event()

    def start(self):
        # Start, calling the run method (RMQ is derived from
        # threading.Thread).
        super().start()

        # Before returning, wait for the RabbitMQ setup to complete.
        if not self._ready.wait(self.ready_wait_interval):
            raise RuntimeError('RabbitMQ initialization took too long!')

    def run(self):
        while not self._stopping:
            # The message counter for publish confirmation is per-channel, so
            # we reset it here, since we're going to create a new channel.
            self._message_number = 0

            # Start connection process.
            self._connection = self._connect()

            # Process IO events: blocks until stopped by a call to
            # ioloop.stop() from another context.
            self._connection.ioloop.start()

    def stop(self):
        # Mark that we want to drop out of the loop in the run() method.
        self._stopping = True

        # Close the channel and connection: closing the connection will
        # eventually cause the I/O loop to exit.
        if self._channel is not None:
            self._channel.close()
        if self._connection is not None:
            self._connection.close()

    def _connect(self):
        logger.info('Connecting to RabbitMQ...')
        return SelectConnection(
            self.parameters,
            on_open_callback=lambda _: self._open_channel(),
            on_open_error_callback=lambda _, err: self._on_connection_open_error(err),
            on_close_callback=lambda _, reason: self._on_connection_closed(reason)
        )

    def _on_connection_open_error(self, err):
        logger.error(
            'RabbitMQ connection open failed, reopening in 5 seconds: %s', err
        )
        # Cause the call to ioloop.start() in the run() method above to
        # return, which causes a reconnection unless self._stopping is true.
        self._connection.ioloop.call_later(
            self.reconnect_interval, self._connection.ioloop.stop
        )

    def _on_connection_closed(self, reason):
        self._channel = None
        if self._stopping:
            # Cause the call to ioloop.start() in the run() method to return
            # immediately, and because self._stopping is true, breaking out of
            # the run loop and terminating the RMQ handler thread.
            self._connection.ioloop.stop()
        else:
            logger.warning(
                'RabbitMQ connection closed, reopening in 5 seconds: %s',
                reason
            )
            # Cause the call to ioloop.start() in the run() method above to
            # return, which causes a reconnection since self._stopping is
            # false.
            self._connection.ioloop.call_later(
                self.reconnect_interval, self._connection.ioloop.stop
            )

    def _open_channel(self):
        logger.info('Opening RabbitMQ channel...')
        self._connection.channel(on_open_callback=self._on_channel_open)

    def _on_channel_open(self, channel):
        self._channel = channel
        self._channel.add_on_close_callback(self._on_channel_closed)
        logger.info('Enabled delivery confirmation on RabbitMQ channel...')
        self._channel.confirm_delivery(
            ack_nack_callback=self._on_delivery_confirmation
        )
        logger.info('Setting QoS on RabbitMQ channel...')
        self._channel.basic_qos(
            prefetch_count=self.prefetch_count,
            callback=lambda _: self._setup_exchange(0)
        )

    def _on_delivery_confirmation(self, frame):
        # When a publish confirmation ACK or NACK message is received, we send
        # a suitable value to the output queue. Application-level code can
        # keep track of published messages with pending acknowledgements to
        # decide whether to resend. Receiving an ACK for a delivery tag of N
        # means that all messages with delivery tags from 1 ... N are
        # acknowledged.
        match frame.method:
            case spec.Basic.Ack():
                self.out_queue.put(RMQ.Ack(frame.method.delivery_tag))
            case spec.Basic.Nack():
                self.out_queue.put(RMQ.Nack(frame.method.delivery_tag))
            case _:
                logger.warning(
                    'unknown delivery confirmation message: %s',
                    str(frame.method)
                )

    def _on_channel_closed(self, channel, reason):
        if not isinstance(reason, ChannelClosedByClient):
            logger.warning('RabbitMQ channel %i was closed: %s', channel, reason)
        self._channel = None
        if not self._stopping and self._connection.is_open:
            self._connection.close()

    def _channel_error(self):
        raise RuntimeError('RabbitMQ sequencing error: unexpected null channel')

    def _consumer_error(self):
        raise RuntimeError(
            'RabbitMQ sequencing error: unexpected null receive information'
        )

    def _setup_exchange(self, idx: int):
        # This method is called repeatedly to set up the exchanges from the
        # list passed into the constructor. All exchanges are created as
        # durable fanout exchanges.

        if self._channel is None:
            self._channel_error()
        logger.info('Declaring RabbitMQ exchange "%s"', self.exchanges[idx])
        self._channel.exchange_declare(
            exchange=self.exchanges[idx],
            exchange_type=ExchangeType.fanout,
            durable=True,
            callback=lambda _: self._on_exchange_declareok(idx)
        )

    def _on_exchange_declareok(self, idx: int):
        if idx < len(self.exchanges) - 1:
            # If there are more exchanges to set up, go back to
            # _setup_exchange.
            self._setup_exchange(idx + 1)
        elif self.consumers is not None and len(self.consumers) > 0:
            # If all exchanges are set up and there are consumers to set up,
            # start on the first one.
            self._setup_consumers(0)
        else:
            # Otherwise, all setup is done and we can signal the start()
            # method to return.
            logger.info('RabbitMQ ready!')
            self._ready.set()

    def _setup_consumers(self, idx: int):
        # This method is called repeatedly to set up the consumers from the
        # `consumers` list passed into the constructor. Each entry in the
        # consumers list gives an exchange name to consume. We create queues
        # with unique names made from the exchange name and the RMQ instance
        # name passed into the constructor. Queues are created as durable or
        # not depending on the `durable` flag in the `Consumer` configuration.

        if self.consumers is None:
            self._consumer_error()
        if self._channel is None:
            self._channel_error()
        consumer = self.consumers[idx]

        # The queue name needs an instance-dependent additional identifier
        # here since there might be multiple consumers attached to the same
        # exchange, and each will need a distinct queue.
        queue_name = consumer.exchange + ':' + self.name
        logger.info('Declaring RabbitMQ queue "%s"...', queue_name)
        self._channel.queue_declare(
            queue=queue_name, durable=consumer.durable,
            callback=lambda _: self._on_queue_declareok(idx)
        )

    def _on_queue_declareok(self, idx: int):
        # Manage queue binding for consumption.

        if self.consumers is None:
            self._consumer_error()
        if self._channel is None:
            self._channel_error()
        consumer = self.consumers[idx]
        queue_name = consumer.exchange + ':' + self.name
        logger.info('Binding RabbitMQ queue "%s"...', queue_name)
        self._channel.queue_bind(
            queue=queue_name, exchange=consumer.exchange,
            callback=lambda _: self._on_queue_bindok(idx)
        )

    def _on_queue_bindok(self, idx: int):
        # Set up consumption.

        if self.consumers is None:
            self._consumer_error()
        if self._channel is None:
            self._channel_error()
        consumer = self.consumers[idx]
        queue_name = consumer.exchange + ':' + self.name
        logger.info('Consuming RabbitMQ queue "%s"...', queue_name)
        self._channel.basic_consume(
            queue=queue_name,
            on_message_callback=functools.partial(self._on_message, consumer)
        )

        if idx < len(self.consumers) - 1:
            # If there are more consumers to set up, go back to
            # _setup_consumers.
            self._setup_consumers(idx + 1)
        else:
            # Otherwise, all setup is done and we can signal the start()
            # method to return.
            logger.info('RabbitMQ ready!')
            self._ready.set()

    def _on_message(self, consumer, ch, method_frame, _header_frame, body):
        # All messages are passed as Protocol Buffers messages, so parse the
        # supplied message class from the message body.
        message = consumer.message_class()
        message.ParseFromString(body)

        # Pass the received message to the outer application context,
        # including information about the receivinig exchange and delivery
        # tag.
        self.out_queue.put((consumer.exchange, method_frame.delivery_tag, message))

        # Acknowledge all messages immediately.
        ch.basic_ack(method_frame.delivery_tag)

    def _send(self, exchange: str, message, persistent: bool = True):
        if self._channel is None:
            self._channel_error()

        if persistent:
            dm = DeliveryMode.Persistent
        else:
            dm = DeliveryMode.Transient

        # Encode Protocol Buffers message and publish.
        self._channel.basic_publish(
            exchange, '', body=message.SerializeToString(),
            properties=spec.BasicProperties(delivery_mode=dm)
        )

    def send(self, exchange: str, message, persistent: bool = True) -> int:
        """Send a Protocol Buffers message to an exchange."""

        # Message number used for publish confirmation: returned to caller for
        # later correlation with ACK/NACK messages.
        self._message_number += 1

        # Thread-safe invocation of method to do actual message sending within
        # I/O loop.
        self._connection.ioloop.add_callback_threadsafe(
            functools.partial(self._send, exchange, message, persistent)
        )
        return self._message_number
