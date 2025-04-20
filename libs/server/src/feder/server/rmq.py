from dataclasses import dataclass
import functools
import logging
from queue import Queue
from threading import Thread, Event, Timer
from typing import Callable, Any
import uuid

from pika import (
    ConnectionParameters, BasicProperties, SelectConnection, spec
)
from pika.exchange_type import ExchangeType
from pika.delivery_mode import DeliveryMode
from pika.exceptions import ChannelClosedByClient
import pika
import pika.channel
import pika.frame

logger = logging.getLogger(__name__)
logging.getLogger('pika').setLevel(logging.WARNING)


@dataclass
class Message:
    """Base class for messages sent to output queue."""
    delivery_tag: int


@dataclass
class AckMessage(Message):
    """ACK message for publish confirmation sent to output queue."""
    ...


@dataclass
class NackMessage(Message):
    """NACK message for publish confirmation sent to output queue."""
    ...


@dataclass
class DataMessage(Message):
    """Received data message sent to output queue."""
    exchange: str
    message: Any


@dataclass
class RPCMessage(Message):
    """RPC request message sent to output queue."""
    endpoint: str
    reply_to: str
    correlation_id: str
    message: Any


@dataclass
class RPCErrorMessage(Message):
    """RPC error message sent to output queue."""
    endpoint: str
    correlation_id: str
    reason: str


@dataclass
class Consumer:
    """Normal message consumer information."""
    exchange: str
    message_class: type
    durable: bool = True


@dataclass
class RPCEndpoint:
    """RPC server endpoint information."""
    name: str
    request_class: type
    response_class: type


type RPCCallback = Callable[[str, type], None]
RPCErrorCallback = Callable[[str, str], None]


@dataclass
class RPCData:
    endpoint: RPCEndpoint
    callback: RPCCallback
    error_callback: RPCErrorCallback | None
    timeout_timer: Timer | None


# The type variable M here is the base message class used for all
# communications.

class RMQ(Thread):
    """Class to manage RabbitMQ interactions for Feder processes.

       Runs all RabbitMQ interactions in a separate thread using an
       asynchronous connection with publish confirmation. Includes a
       simplified setup mechanism for exchanges, queues and consumers.

       Note:

        - All exchanges are durable (except for the RPC exchange), but queues
          for consumers can be set up as durable or not depending on what's
          needed.
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
      - The RPC client and server use the "rpc" exchange, which is handled
        separately from the "normal" message exchanges and is a non-durable
        direct exchange.
      - RPC requests are routed using the endpoint name as the routing key.
      - Correlation of RPC requests and replies is done via unique correlation
        IDs generated when a request is made.
    """
    RPC_EXCHANGE = 'rpc'

    def __init__(
            self,
            name: str,
            parameters: ConnectionParameters,
            out_queue: Queue,
            message_class: type,
            exchanges: list[str],
            consumers: list[Consumer] | None = None,
            wrapper_class: type | None = None,
            rpc_client: bool = True,
            rpc_server: list[str] | None = None,
            rpc_endpoints: list[RPCEndpoint] | None = None,
            prefetch_count: int = 1,
            reconnect_interval: float = 5.0,
            ready_wait_interval: float = 5.0,
            *args, **kwargs):
        """Initialise RabbitMQ infrastructure.

        :param str name: Name used to make unique RabbitMQ queue names
        :param pika.ConnectionParameters parameters: Connection parameters
        :param queue.Queue out_queue: Output queue for consumption messages,
            publish confirmation ACK/NACK messages and RPC messages
        :param type message_class: Base class for all messages sent.
        :param list[str] exchanges: List of RabbitMQ exchanges to create
        :param list[Consumer] | None consumers: Consumption configuration
            associating exchange names with Protocol Buffers message types for
            RabbitMQ message consumption
        :param type | None wrapper_class: Message consumption wrapper class
        :param bool rpc_client: Enable RPC client operation
        :param list[str] | None rpc_server: List of RPC endpoints served
        :param list[RPCEndpoint] | None rpc_endpoints: RPC endpoint
            definitions
        :param int prefetch_count: QoS prefetch count
        :param float reconnect_interval: Time to wait (s) to reconnect to
            RabbitMQ after connection failure
        :param float ready_wait_interval: Time to wait (s) to for RabbitMQ
            infrastructure setup when starting RabbitMQ handler thread

        """
        super().__init__(*args, **kwargs)

        self.name = name
        self.parameters = parameters
        self.out_queue = out_queue
        self.base_message_class = message_class
        self.wrapper_class = wrapper_class
        self.exchanges = exchanges
        self.consumers = consumers or []
        self.rpc_client = rpc_client
        self.rpc_server = rpc_server
        self.rpc_endpoints = rpc_endpoints
        self.prefetch_count = prefetch_count
        self.reconnect_interval = reconnect_interval
        self.ready_wait_interval = ready_wait_interval

        # RabbitMQ top-level entities.
        self._connection: pika.SelectConnection | None = None
        self._channel: pika.channel.Channel | None = None
        self._rpc_channel: pika.channel.Channel | None = None

        # IO loop termination control.
        self._stopping = False

        # Message counter for publish confirmation.
        self._message_number = 0

        # Dictionary to map correlation IDs to callbacks for RPC client.
        self._rpc_callbacks: dict[str, RPCData] = {}

        # Dictionary to map RPC server endpoints to message class pairs.
        self._rpc_endpoints_by_name = {
            ep.name: ep for ep in (self.rpc_endpoints or [])
        }

        # Event to wait for RabbitMQ initialisation.
        self._ready = Event()

        # Check input values for exchanges and consumers.
        self._check_exchanges()
        self._check_consumers()
        self._check_rpc_server()

        # Determine required setup steps.
        self._setup_steps = self._calculate_setup_steps()

    def start(self):
        """Thread start implementation: starts the RabbitMQ handler thread."""
        super().start()

        # Before returning, wait for the RabbitMQ setup to complete.
        if not self._ready.wait(self.ready_wait_interval):
            raise RuntimeError('RabbitMQ initialization took too long!')

    def run(self):
        """Main thread function: continuously reconnects to RabbitMQ and runs
          event loop, reinitializing RabbitMQ entities on each reconnection.
        """
        while not self._stopping:
            # The message counter for publish confirmation is per-channel, so
            # we reset it here, since we're going to create a new channel. We
            # also remove any pending RPC callback mappings.
            self._message_number = 0
            self._rpc_callbacks = {}

            # Start connection process. The connection open callback starts
            # the RabbitMQ entity setup process.
            logger.info('Connecting to RabbitMQ...')
            self._connection = SelectConnection(
                self.parameters,
                on_open_callback=lambda _: self._setup(),
                on_open_error_callback=lambda _, err: self._on_connection_open_error(err),
                on_close_callback=lambda _, reason: self._on_connection_closed(reason)
            )

            # Process IO events: blocks until stopped by a call to
            # ioloop.stop() from another context.
            try:
                self._connection.ioloop.start()
            except Exception:
                if self._stopping:
                    # If we're stopping, just break out of the loop, because
                    # this is likely to be a "channel in wrong state" error.
                    break
                raise

    def stop(self):
        """Stop the RabbitMQ event loop."""

        # Mark that we want to drop out of the loop in the run() method.
        self._stopping = True

        # Close the channel and connection: closing the connection will
        # eventually cause the I/O loop to exit.
        if self._channel is not None:
            self._channel.close()
        if self._rpc_channel is not None:
            self._rpc_channel.close()
        if self._connection is not None:
            self._connection.close()

    # All of these message sending functions rely on calling the
    # add_callback_threadsafe method on the I/O loop to inject a call to an
    # implementation method into the I/O loop in the RabbitMQ handler thread.
    # This is the only approved thread-safe method of invoking pika functions
    # from another thread.

    def send(
            self,
            exchange: str,
            message: Any,
            persistent: bool = True
    ) -> int:
        """Send a Protocol Buffers message to an exchange."""

        self._check_ready()

        if not isinstance(message, self.base_message_class):
            raise ValueError('bad message type')
        try:
            packed_message = message.pack()
        except Exception as exc:
            print(exc)
            raise ValueError('message type does not support packing')

        # Message number used for publish confirmation: returned to caller for
        # later correlation with ACK/NACK messages.
        self._message_number += 1

        # Thread-safe invocation of method to do actual message sending within
        # I/O loop.
        self._connection.ioloop.add_callback_threadsafe(
            functools.partial(
                self._send, exchange, packed_message, persistent
            )
        )
        return self._message_number

    def send_rpc(
            self,
            endpoint: str,
            payload: Any,
            callback: RPCCallback,
            error_callback: RPCErrorCallback | None = None,
            timeout: int = None
    ) -> str:
        """Send an RPC request Protocol Buffers message to an endpoint."""

        self._check_ready()
        if not self.rpc_client:
            raise RuntimeError('RPC client operation is not configured')
        if endpoint not in self._rpc_endpoints_by_name:
            raise ValueError(f'unknown RPC endpoint: {endpoint}')
        ep_data = self._rpc_endpoints_by_name[endpoint]
        if not isinstance(payload, ep_data.request_class):
            raise ValueError(
                f'invalid request class "{type(payload)}" '
                f'for RPC endpoint "{endpoint}"'
            )
        try:
            packed_payload = payload.pack()
        except Exception:
            raise ValueError('payload type does not support packing')


        # Generate unique correlation ID for request and save callback for
        # reply processing.
        correlation_id = str(uuid.uuid4())
        timeout_timer = None
        if timeout is not None:
            timeout_timer = Timer(timeout, lambda: self._rpc_timeout(correlation_id))
            timeout_timer.start()
        self._rpc_callbacks[correlation_id] = RPCData(
            ep_data, callback, error_callback, timeout_timer
        )

        # Message number used for publish confirmation: returned to caller for
        # later correlation with ACK/NACK messages.
        self._message_number += 1

        # Thread-safe invocation of method to do actual message sending within
        # I/O loop.
        self._connection.ioloop.add_callback_threadsafe(
            functools.partial(
                self._send_rpc, endpoint, packed_payload, correlation_id
            )
        )

        return correlation_id

    def cancel_rpc(self, correlation_id: str):
        if correlation_id in self._rpc_callbacks:
            timeout_timer = self._rpc_callbacks[correlation_id].timeout_timer
            if timeout_timer is not None:
                timeout_timer.cancel()
            del self._rpc_callbacks[correlation_id]

    def rpc_reply(
            self,
            request_message: RPCMessage,
            reply_message: Any
    ) -> int:
        """Send a reply to an RPC invocation."""

        self._check_ready()
        if not self.rpc_server:
            raise RuntimeError('RPC server operation is not configured')
        endpoint = request_message.endpoint
        if endpoint not in self._rpc_endpoints_by_name:
            raise ValueError(f'unknown RPC endpoint: {endpoint}')
        if endpoint not in self.rpc_server:
            raise ValueError(
                f'RPC server for endpoint "{endpoint}" is not configured'
            )
        ep_data = self._rpc_endpoints_by_name[endpoint]
        if not isinstance(reply_message, ep_data.response_class):
            raise ValueError(
                f'invalid response class "{type(reply_message)}" '
                f'for RPC endpoint "{endpoint}"'
            )

        try:
            packed_reply_message = reply_message.pack()
        except Exception:
            raise ValueError('reply message type does not support packing')

        # Message number used for publish confirmation: returned to caller for
        # later correlation with ACK/NACK messages. I think we need to
        # increment this here even though we don't use the message number,
        # just to keep our count aligned with pika's count.
        self._message_number += 1

        # Thread-safe invocation of method to do actual message sending within
        # I/O loop.
        self._connection.ioloop.add_callback_threadsafe(
            functools.partial(
                self._rpc_reply, request_message, packed_reply_message
            )
        )
        return self._message_number

    #----------------------------------------------------------------------------
    #
    #  CONSTRUCTOR HELPERS
    #
    #----------------------------------------------------------------------------

    # There are some preconditions that have to be satisfied by the exchanges,
    # consumers and RPC endpoint parameters to the constructor. These methods
    # check the following conditions:
    #
    # _check_exchanges: Check that exchange names are unique and that the
    # "rpc" exchange is not included in the list of exchange names. If RPC is
    # not enabled, also check that at least one "normal" exchange is given.
    #
    # _check_consumers: Check that consumer exchange names are unique, that
    # the exchange names are included in the exchange name list, and that all
    # of the message classes provided are valid Protocol Buffers message
    # classes.
    #
    # _check_rpc_server: Check that the RPC server endpoint names are included
    # in the rpc_endpoints endpoint list passed to the constructor.

    def _check_exchanges(self):
        # We can't do anything without an exchange and we need the requested
        # exchanges to be unique.
        if len(self.exchanges) == 0 and not self.rpc_client and not self.rpc_server:
            raise ValueError('no exchange names provided for RMQ')
        if len(set(self.exchanges)) != len(self.exchanges):
            raise ValueError('exchange names for RMQ must be unique')
        if self.RPC_EXCHANGE in self.exchanges:
            raise ValueError(f'not allowed to use exchange name "{self.RPC_EXCHANGE}"')

    def _check_consumers(self):
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
                if not issubclass(c.message_class, self.base_message_class):
                    raise ValueError(
                        f'receive class "{c.message_class}" not a '
                        'subclass of the base message class'
                    )

    def _check_rpc_server(self):
        # The list of endpoints in the rpc_server parameter to the constructor
        # should correspond to endpoints in the rpc_endpoints parameter.
        if self.rpc_server is not None:
            for s in self.rpc_server:
                if s not in self._rpc_endpoints_by_name:
                    raise ValueError(f'unknown RPC endpoint "{s}" for RPC server')

    #----------------------------------------------------------------------------
    #
    #  INSTANCE SETUP CONTROL
    #
    #----------------------------------------------------------------------------

    # RMQ instance steps is done by a pre-computed sequence of initialization
    # steps calculated in the _calculate_setup_steps() method. Each step is
    # either a single (bound) method or a (bound) method and an argument
    # value. At each step, the method is called with the current step index
    # and the provided argument (if any).
    #
    # Steps are chained together by calling the _setup() method with the
    # current step index. This method is responsible for the sequencing of the
    # initialization steps.
    #
    # When initialization is complete, the _ready Event is signalled to mark
    # that the RMQ instance is ready to use.

    def _calculate_setup_steps(self):
        # Fixed channel setup steps.
        steps = [self._open_channel]
        if self.rpc_client or self.rpc_server:
            steps.append(self._open_rpc_channel)

        # Declare requested exchanges.
        steps += [
            (self._declare_exchange, exch) for exch in self.exchanges
        ]

        # If RPC is being used, declare the RPC exchange.
        if self.rpc_client or self.rpc_server:
            steps.append((self._declare_exchange, self.RPC_EXCHANGE))

        # Set up requested consumers.
        for step in [
                self._declare_consumer_queue,
                self._bind_consumer_queue,
                self._consume_consumer_queue
        ]:
            steps += [(step, cons) for cons in self.consumers]

        # Set up queue for replies to RPC client requests.
        if self.rpc_client:
            steps += [
                self._declare_rpc_client_reply_queue,
                self._bind_rpc_client_reply_queue,
                self._consume_rpc_client_reply_queue
            ]

        if self.rpc_server:
            # Set up RPC server endpoints. We want one queue with multiple
            # bindings using the RPC endpoint name as the routing key.
            steps.append(self._declare_rpc_server_queue)
            steps += [
                (self._bind_rpc_server_queue, self._rpc_endpoints_by_name[ep])
                for ep in self.rpc_server
            ]
            steps.append(self._consume_rpc_server_queue)

        return steps

    def _setup(self, last_step_idx: int = -1):
        # Called repeatedly to set up RabbitMQ entities.

        step_idx = last_step_idx + 1

        if step_idx < len(self._setup_steps):
            # The _setup_steps list contains either a method or a tuple of
            # method and argument. All of the step methods take the step index
            # as a first argument.
            step_data = self._setup_steps[step_idx]
            if isinstance(step_data, tuple):
                # If the step is a tuple, it's a setup step with an argument.
                step_fn = step_data[0]
                step_args = [step_data[1]]
            else:
                # Otherwise, it's just a setup step with no arguments.
                step_fn = step_data
                step_args = []

            step_fn(step_idx, *step_args)
        else:
            # All setup is done and we can signal the start() method to return.
            logger.info('RabbitMQ ready!')
            self._ready.set()

    #----------------------------------------------------------------------------
    #
    #  INSTANCE SETUP STEPS
    #
    #----------------------------------------------------------------------------

    def _open_channel(self, step_idx: int):
        logger.info('Opening RabbitMQ channel...')
        assert self._connection is not None
        self._connection.channel(
            on_open_callback=lambda ch: self._on_channel_open(step_idx, ch)
        )

    def _on_channel_open(self, step_idx: int, channel: pika.channel.Channel):
        self._channel = channel
        assert self._channel is not None
        self._channel.add_on_close_callback(self._on_channel_closed)
        logger.info('Enable delivery confirmation on RabbitMQ channel...')
        self._channel.confirm_delivery(
            ack_nack_callback=self._on_delivery_confirmation
        )
        logger.info('Setting QoS on RabbitMQ channel...')
        self._channel.basic_qos(
            prefetch_count=self.prefetch_count,
            callback=lambda _: self._setup(step_idx)
        )

    def _open_rpc_channel(self, step_idx: int):
        logger.info('Opening RabbitMQ RPC channel...')
        assert self._connection is not None
        self._connection.channel(
            on_open_callback=lambda ch: self._on_rpc_channel_open(step_idx, ch)
        )

    def _on_rpc_channel_open(self, step_idx: int, channel: pika.channel.Channel):
        self._rpc_channel = channel
        assert self._rpc_channel is not None
        self._rpc_channel.add_on_close_callback(self._on_rpc_channel_closed)
        logger.info('Setting QoS on RabbitMQ RPC channel...')
        self._rpc_channel.basic_qos(
            prefetch_count=self.prefetch_count,
            callback=lambda _: self._setup(step_idx)
        )

    def _declare_exchange(self, step_idx: int, exchange: str):
        # This method is called repeatedly to set up the exchanges from the
        # list passed into the constructor. All "normal" exchanges are created
        # as durable fanout exchanges.

        logger.info('Declaring RabbitMQ exchange "%s"', exchange)

        # For normal consumers, we use a durable fanout exchange. For RPC, we
        # use a transient direct exchange to route requests by "method name".
        match exchange:
            case self.RPC_EXCHANGE:
                exchange_type = ExchangeType.direct
                durable = False
                channel = self._rpc_channel
            case _:
                exchange_type = ExchangeType.fanout
                durable = True
                channel = self._channel

        assert channel is not None
        channel.exchange_declare(
            exchange=exchange,
            exchange_type=exchange_type,
            durable=durable,
            callback=lambda _: self._setup(step_idx)
        )

    def _declare_consumer_queue(self, step_idx: int, consumer: Consumer):
        # This method is called repeatedly to set up the consumers from the
        # `consumers` list passed into the constructor. Each entry in the
        # consumers list gives an exchange name to consume. We create queues
        # with unique names made from the exchange name and the RMQ instance
        # name passed into the constructor. Queues are created as durable or
        # not depending on the `durable` flag in the `Consumer` configuration.

        # The queue name needs an instance-dependent additional identifier
        # here since there might be multiple consumers attached to the same
        # exchange, and each will need a distinct queue.
        queue_name = consumer.exchange + ':' + self.name
        logger.info('Declaring RabbitMQ queue "%s"...', queue_name)
        assert self._channel is not None
        self._channel.queue_declare(
            queue=queue_name, durable=consumer.durable,
            callback=lambda _: self._setup(step_idx)
        )

    def _bind_consumer_queue(self, step_idx: int, consumer: Consumer):
        # Manage queue binding for consumption.

        assert self._channel is not None
        queue_name = consumer.exchange + ':' + self.name
        logger.info('Binding RabbitMQ queue "%s"...', queue_name)
        self._channel.queue_bind(
            queue=queue_name, exchange=consumer.exchange,
            callback=lambda _: self._setup(step_idx)
        )

    def _consume_consumer_queue(self, step_idx: int, consumer: Consumer):
        # Set up consumption.

        queue_name = consumer.exchange + ':' + self.name
        logger.info('Consuming RabbitMQ queue "%s"...', queue_name)
        assert self._channel is not None
        self._channel.basic_consume(
            queue=queue_name,
            on_message_callback=functools.partial(self._on_message, consumer)
        )
        self._setup(step_idx)

    def _declare_rpc_server_queue(self, step_idx: int):
        queue_name = self.RPC_EXCHANGE + ':' + self.name
        logger.info('Declaring RabbitMQ queue "%s"...', queue_name)
        assert self._rpc_channel is not None
        self._rpc_channel.queue_declare(
            queue=queue_name, callback=lambda _: self._setup(step_idx)
        )

    def _bind_rpc_server_queue(self, step_idx: int, endpoint: RPCEndpoint):
        # Manage queue binding for RPC server endpoint.

        assert self._rpc_channel is not None
        queue_name = self.RPC_EXCHANGE + ':' + self.name
        logger.info(
            'Binding RabbitMQ queue "%s" at endpoint "%s"...',
            queue_name, endpoint.name
        )
        self._rpc_channel.queue_bind(
            queue=queue_name, exchange=self.RPC_EXCHANGE,
            routing_key=endpoint.name,
            callback=lambda _: self._setup(step_idx)
        )

    def _consume_rpc_server_queue(self, step_idx: int):
        # Set up consumption for RPC server endpoint.

        queue_name = self.RPC_EXCHANGE + ':' + self.name
        logger.info(
            'Consuming RabbitMQ queue "%s"...', queue_name
        )

        # Don't require acknowledgements for RPC traffic.
        assert self._rpc_channel is not None
        self._rpc_channel.basic_consume(
            queue=queue_name,
            on_message_callback=self._on_rpc_request,
            auto_ack=True
        )
        self._setup(step_idx)

    # TODO: Delete this queue on exit.

    def _declare_rpc_client_reply_queue(self, step_idx: int):
        # An empty queue name here causes the broker to autogenerate a name,
        # and "exclusive" means the queue goes away when the connection goes
        # away (the Pika docs say "Only allow access by the current
        # connection").
        logger.info('Declaring RabbitMQ RPC client reply queue...')
        assert self._rpc_channel is not None
        self._rpc_channel.queue_declare(
            queue='', exclusive=True,
            callback=lambda frame: self._on_rpc_client_queue_declareok(step_idx, frame)
        )

    def _on_rpc_client_queue_declareok(self, step_idx: int, frame: pika.frame.Method):
        self.rpc_callback_queue = frame.method.queue
        self._setup(step_idx)

    def _bind_rpc_client_reply_queue(self, step_idx: int):
        # Manage queue binding for consumption.

        assert self._channel is not None
        logger.info(
            'Binding RabbitMQ RPC reply queue "%s"...',
            self.rpc_callback_queue
        )
        self._channel.queue_bind(
            queue=self.rpc_callback_queue, exchange=self.RPC_EXCHANGE,
            routing_key=self.rpc_callback_queue,
            callback=lambda _: self._setup(step_idx)
        )

    def _consume_rpc_client_reply_queue(self, step_idx: int):
        logger.info(
            'Consuming RabbitMQ RPC reply queue "%s"...', self.rpc_callback_queue
        )

        # Don't require acknowledgements for RPC traffic.
        assert self._rpc_channel is not None
        self._rpc_channel.basic_consume(
            queue=self.rpc_callback_queue,
            on_message_callback=self._on_rpc_response,
            auto_ack=True
        )

        self._setup(step_idx)

    #----------------------------------------------------------------------------
    #
    #  CONNECTION/CHANNEL EVENT HANDLERS
    #
    #----------------------------------------------------------------------------

    def _on_connection_open_error(self, err):
        logger.error(
            'RabbitMQ connection open failed, reopening in 5 seconds: %s', err
        )
        # Cause the call to ioloop.start() in the run() method above to
        # return, which causes a reconnection unless self._stopping is true.
        assert self._connection is not None
        self._connection.ioloop.call_later(
            self.reconnect_interval, self._connection.ioloop.stop
        )

    def _on_connection_closed(self, reason):
        self._channel = None
        self._rpc_channel = None
        assert self._connection is not None
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

    def _on_channel_closed(self, channel, reason):
        if not isinstance(reason, ChannelClosedByClient):
            logger.warning('RabbitMQ channel %i was closed: %s', channel, reason)
        assert self._connection is not None
        self._channel = None
        if self._rpc_channel is not None and self._rpc_channel.is_open:
            self._rpc_channel.close()
        if not self._stopping and self._connection.is_open:
            self._connection.close()

    def _on_rpc_channel_closed(self, channel, reason):
        if not isinstance(reason, ChannelClosedByClient):
            logger.warning('RabbitMQ channel %i was closed: %s', channel, reason)
        assert self._connection is not None
        self._rpc_channel = None
        if self._channel is not None and self._channel.is_open:
            self._channel.close()
        if not self._stopping and self._connection.is_open:
            self._connection.close()

    #----------------------------------------------------------------------------
    #
    #  MESSAGE RECEIPT HANDLERS
    #
    #----------------------------------------------------------------------------

    def _on_message(self, consumer: Consumer, _ch, method, props, body):
        # All messages are passed using a message class hierarchy that knows
        # how to pack and unpack messages, so parse the supplied message class
        # from the message body.
        try:
            message = self.base_message_class.unpack(body)
            if not isinstance(message, consumer.message_class):
                raise ValueError(
                    f'incorrect message class "{type(message)}"'
                )
        except Exception:
            logger.exception(
                'error unpacking message body for exchange "%s"',
                consumer.exchange
            )
            self._channel.basic_nack(method.delivery_tag)
            return

        # Pass the received message to the outer application context,
        # including information about the receivinig exchange and delivery
        # tag.
        self.out_queue.put(self._wrap(
            DataMessage(
                delivery_tag=method.delivery_tag,
                exchange=consumer.exchange,
                message=message
            )
        ))

        # Acknowledge all messages immediately.
        assert self._channel is not None
        self._channel.basic_ack(method.delivery_tag)

    def _on_rpc_request(self, _ch, method, props, body):
        endpoint_name = method.routing_key
        if endpoint_name not in self._rpc_endpoints_by_name:
            raise ValueError(f'unknown RPC endpoint: {endpoint_name}')
        endpoint = self._rpc_endpoints_by_name[endpoint_name]
        try:
            message = self.base_message_class.unpack(body)
            if not isinstance(message, endpoint.request_class):
                raise ValueError(
                    f'incorrect RPC request class "{type(message)}"'
                )
        except Exception as err:
            self._rpc_channel.basic_nack(method.delivery_tag)
            self.out_queue.put(self._wrap(
                RPCErrorMessage(
                    delivery_tag=method.delivery_tag,
                    endpoint=endpoint.name,
                    correlation_id=props.correlation_id,
                    reason=f'failed to decode RPC request message: {err}'
                )
            ))
            return

        # Pass RPC request to outer application context, including
        # correlation ID and reply queue, and keeping track of the
        # association between them for later reply processing.
        self.out_queue.put(self._wrap(
            RPCMessage(
                delivery_tag=method.delivery_tag,
                endpoint=endpoint.name,
                reply_to=props.reply_to,
                correlation_id=props.correlation_id,
                message=message
            )
        ))

        # Message will be acknowledged when a response is sent.

    def _on_rpc_response(self, _ch, method, props, body):
        # Ignore responses for any timed out or cancelled requests.
        rpc_data = self._rpc_callbacks.get(props.correlation_id)
        if rpc_data is None:
            return

        if rpc_data.timeout_timer is not None:
            rpc_data.timeout_timer.cancel()

        # Parse the response using the endpoint's response message class.
        try:
            response = self.base_message_class.unpack(body)
            if not isinstance(response, rpc_data.endpoint.response_class):
                raise ValueError(
                    f'incorrect RPC response class "{type(response)}"'
                )
        except Exception as err:
            self._rpc_channel.basic_nack(method.delivery_tag)
            if rpc_data.error_callback is not None:
                rpc_data.error_callback(
                    props.correlation_id,
                    f'failed to decode RPC response message: {err}'
                )
            return
        finally:
            del self._rpc_callbacks[props.correlation_id]

        try:
            rpc_data.callback(props.correlation_id, response)
        except Exception:
            # Ignore exceptions here: if the client throws an exception on a
            # response, there's nothing we can do anyway.
            pass

    def _on_delivery_confirmation(self, frame):
        # When a publish confirmation ACK or NACK message is received, we send
        # a suitable value to the output queue. Application-level code can
        # keep track of published messages with pending acknowledgements to
        # decide whether to resend. Receiving an ACK for a delivery tag of N
        # means that all messages with delivery tags from 1 ... N are
        # acknowledged.
        match frame.method:
            case spec.Basic.Ack():
                self.out_queue.put(
                    self._wrap(AckMessage(frame.method.delivery_tag))
                )
            case spec.Basic.Nack():
                self.out_queue.put(
                    self._wrap(NackMessage(frame.method.delivery_tag))
                )
            case _:
                logger.warning(
                    'unknown delivery confirmation message: %s',
                    str(frame.method)
                )

    #----------------------------------------------------------------------------
    #
    #  MESSAGE SEND IMPLEMENTATIONS
    #
    #----------------------------------------------------------------------------

    def _send(
            self,
            exchange: str,
            serialized_message: str,
            persistent: bool = True
    ):
        # Encode Protocol Buffers message and publish.
        assert self._channel is not None
        dm = DeliveryMode.Persistent if persistent else DeliveryMode.Transient
        self._channel.basic_publish(
            exchange, '', body=serialized_message,
            properties=spec.BasicProperties(delivery_mode=dm)
        )

    def _send_rpc(
            self,
            endpoint: str,
            serialized_payload: str,
            correlation_id: str
    ):
        assert self._rpc_channel is not None
        self._rpc_channel.basic_publish(
            exchange=self.RPC_EXCHANGE,
            routing_key=endpoint,
            properties=BasicProperties(
                reply_to=self.rpc_callback_queue,
                correlation_id=correlation_id
            ),
            body=serialized_payload
        )

    def _rpc_reply(
            self,
            request_message: RPCMessage,
            serialized_reply_message: str
    ):
        # Encode Protocol Buffers message and publish on the RPC exchange
        # using the routing key to send the reply back to the requester.
        assert self._rpc_channel is not None
        self._rpc_channel.basic_publish(
            exchange=self.RPC_EXCHANGE,
            routing_key=request_message.reply_to,
            body=serialized_reply_message,
            properties=spec.BasicProperties(
                delivery_mode=DeliveryMode.Transient,
                correlation_id=request_message.correlation_id
            )
        )

    #----------------------------------------------------------------------------
    #
    #  UTILITY METHODS
    #
    #----------------------------------------------------------------------------

    def _wrap(self, msg):
        # Wrap messages if a wrapper class is provided.
        if self.wrapper_class is not None:
            return self.wrapper_class(msg)
        return msg

    def _check_ready(self):
        # Check for ready state for communication methods.
        if (
                self._connection is None or
                self._channel is None or
                not self._ready.is_set()
        ):
            raise RuntimeError('RMQ instance is not ready')

    def _rpc_timeout(self, correlation_id: str):
        if correlation_id not in self._rpc_callbacks:
            return
        rpc_data = self._rpc_callbacks[correlation_id]
        if rpc_data.error_callback is not None:
            rpc_data.error_callback(correlation_id, 'TIMEOUT')
        del self._rpc_callbacks[correlation_id]
