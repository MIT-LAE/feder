from dataclasses import dataclass
from enum import Enum
import functools
import logging
from queue import Queue
from threading import Thread, Event
from typing import Any
import uuid

import google.protobuf.message as pbmsg
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


class MessageType(Enum):
    DATA = 1
    ACK = 2
    NACK = 3
    RPC = 4


# TODO: The routing of RPC requests to their handlers isn't very good. Need
# something better here.

# TODO: Disable ACK/NACK for RPC requests and replies.

# TODO: Make this a set of classes, rather than a single "everything together"
# dataclass like this?
@dataclass
class Message:
    message_type: MessageType
    delivery_tag: int
    endpoint: str = ''
    reply_to: str = ''
    correlation_id: str = ''
    exchange: str = ''
    message: type[pbmsg.Message] | None = None

    @classmethod
    def ack(cls, method):
        return cls(
            message_type=MessageType.ACK,
            delivery_tag=method.delivery_tag
        )

    @classmethod
    def nack(cls, method):
        return cls(
            message_type=MessageType.NACK,
            delivery_tag=method.delivery_tag
        )

    @classmethod
    def rpc(cls, method, correlation_id, reply_to, message):
        return cls(
            message_type=MessageType.RPC,
            exchange=RMQ.RPC_EXCHANGE,
            endpoint=method.routing_key,
            reply_to=reply_to,
            correlation_id=correlation_id,
            delivery_tag=method.delivery_tag,
            message=message
        )

    @classmethod
    def data(cls, consumer, method, message):
        return cls(
            message_type=MessageType.DATA,
            exchange=consumer.exchange,
            delivery_tag=method.delivery_tag,
            message=message
        )


@dataclass
class Consumer:
    """Consumer information."""
    exchange: str
    message_class: type
    durable: bool = True


@dataclass
class RPCEndpoint:
    """RPC server endpoint information."""
    name: str
    request_class: type
    response_class: type


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
    RPC_EXCHANGE = 'rpc'

    def __init__(
            self,
            name: str,
            parameters: ConnectionParameters,
            out_queue: Queue,
            exchanges: list[str],
            consumers: list[Consumer] | None = None,
            wrapper_class: type | None = None,
            rpc_client: bool = True,
            rpc_server: list[RPCEndpoint] | None = None,
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
        :param type | None wrapper_class: Optional wrapper class for message
            consumption.
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
        self.wrapper_class = wrapper_class
        self.exchanges = exchanges
        self.consumers = consumers or []
        self.rpc_client = rpc_client
        self.rpc_server = rpc_server
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
        # self._rpc_callbacks: dict[str, tuple[callable[something...], type[pbmsg.Message]]] = {}
        self._rpc_callbacks: dict[str, tuple[Any, type[pbmsg.Message]]] = {}

        # Event to wait for RabbitMQ initialisation.
        self._ready = Event()

        # Check input values for exchanges and consumers.
        self._check_exchanges()
        self._check_consumers()

        # Determine required setup steps.
        self._setup_steps = self._calculate_setup_steps()

        self._setup_rpc_entities()

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
            for step in [
                    self._bind_rpc_server_queue,
                    self._consume_rpc_server_queue
            ]:
                steps += [(step, ep) for ep in self.rpc_server]

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
            # All setup done: signal start() to return.
            self._all_ready()


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
            # we reset it here, since we're going to create a new channel. We
            # also remove any pending RPC callback mappings.
            self._message_number = 0
            self._rpc_callbacks = {}

            # Start connection process.
            #
            # TODO: Is there a way to make all this asynchronous stuff easier
            # to handle? Maybe break the chain of initializations up, or use a
            # more principled state machine representation? We have connection
            # setup, channel setup, exchange declarations, consumer queue
            # declarations, binding and consumption setup, RPC client response
            # queue setup and RPC server endpoint setup, depending on the
            # parameters passed to the RMQ constructor...
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

    def send(
            self,
            exchange: str,
            message: pbmsg.Message,
            persistent: bool = True
    ) -> int:
        """Send a Protocol Buffers message to an exchange."""

        assert self._connection is not None

        # Message number used for publish confirmation: returned to caller for
        # later correlation with ACK/NACK messages.
        self._message_number += 1

        # Thread-safe invocation of method to do actual message sending within
        # I/O loop.
        self._connection.ioloop.add_callback_threadsafe(
            functools.partial(self._send, exchange, message, persistent)
        )
        return self._message_number

    def send_rpc(
            self,
            endpoint: str,
            payload: pbmsg.Message,
            response_class: type[pbmsg.Message],
            callback
    ) -> str:
        """Send a Protocol Buffers message to an RPC endpoint."""

        if not self.rpc_client:
            raise RuntimeError('RPC client operation is not configured')
        assert self._connection is not None

        # Generate unique correlation ID for request and save callback for
        # reply processing.
        correlation_id = str(uuid.uuid4())
        self._rpc_callbacks[correlation_id] = (callback, response_class)

        # Message number used for publish confirmation: returned to caller for
        # later correlation with ACK/NACK messages.
        self._message_number += 1

        # Thread-safe invocation of method to do actual message sending within
        # I/O loop.
        self._connection.ioloop.add_callback_threadsafe(
            functools.partial(
                self._send_rpc, endpoint, payload, correlation_id
            )
        )

        return correlation_id

    def rpc_reply(
            self,
            request_message: Message,
            reply_message: pbmsg.Message
    ) -> int:
        """Send a reply to an RPC invocation."""

        if not self.rpc_server:
            raise RuntimeError('RPC server operation is not configured')
        assert self._connection is not None

        # Message number used for publish confirmation: returned to caller for
        # later correlation with ACK/NACK messages. I think we need to
        # increment this here even though we don't use the message number,
        # just to keep our count aligned with pika's count.
        self._message_number += 1

        # Thread-safe invocation of method to do actual message sending within
        # I/O loop.
        self._connection.ioloop.add_callback_threadsafe(
            functools.partial(self._rpc_reply, request_message, reply_message)
        )
        return self._message_number


    def _check_exchanges(self):
        # We can't do anything without an exchange and we need the requested
        # exchanges to be unique.
        if len(self.exchanges) == 0:
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
                if not issubclass(c.message_class, pbmsg.Message):
                    raise ValueError(
                        f'receive class "{c.message_class}" not a Protocol Buffers message'
                    )

    def _setup_rpc_entities(self):
        # List of RPC server endpoint names.
        self._rpc_endpoint_names = [ep.name for ep in (self.rpc_server or [])]

        # Dictionary to map RPC server endpoints to message class pairs.
        self._rpc_server_classes = {
            ep.name: (ep.request_class, ep.response_class)
            for ep in (self.rpc_server or [])
        }

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

    def _wrap(self, msg):
        if self.wrapper_class is not None:
            return self.wrapper_class(msg)
        return msg

    def _on_delivery_confirmation(self, frame):
        # When a publish confirmation ACK or NACK message is received, we send
        # a suitable value to the output queue. Application-level code can
        # keep track of published messages with pending acknowledgements to
        # decide whether to resend. Receiving an ACK for a delivery tag of N
        # means that all messages with delivery tags from 1 ... N are
        # acknowledged.
        match frame.method:
            case spec.Basic.Ack():
                self.out_queue.put(self._wrap(Message.ack(frame.method)))
            case spec.Basic.Nack():
                self.out_queue.put(self._wrap(Message.nack(frame.method)))
            case _:
                logger.warning(
                    'unknown delivery confirmation message: %s',
                    str(frame.method)
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

    def _consume_rpc_server_queue(self, step_idx: int, endpoint: RPCEndpoint):
        # Set up consumption for RPC server endpoint.

        queue_name = self.RPC_EXCHANGE + ':' + self.name
        logger.info(
            'Consuming RabbitMQ queue "%s" at endpoint "%s"...',
            queue_name, endpoint.name
        )
        assert self._rpc_channel is not None
        self._rpc_channel.basic_consume(
            queue=queue_name,
            on_message_callback=functools.partial(self._on_rpc_request, endpoint)
        )
        self._setup(step_idx)

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

        # On the *server* side, we only want to ACK after we've handled the
        # request, but on the client side, no-one really cares so we use
        # auto-ACK.
        # TODO: Remove all ACK/NACK stuff for RPC requests and replies?
        assert self._rpc_channel is not None
        self._rpc_channel.basic_consume(
            queue=self.rpc_callback_queue,
            on_message_callback=self._on_rpc_response,
            auto_ack=True
        )

        self._setup(step_idx)

    def _all_ready(self):
        # All setup is done and we can signal the start() method to return.
        logger.info('RabbitMQ ready!')
        self._ready.set()

    def _on_rpc_response(self, _ch, _method, props, body):
        # Ignore responses for any timed out or cancelled requests.
        if props.correlation_id not in self._rpc_callbacks:
            return

        # Parse the response using the supplied response Protocol Buffers
        # class.
        callback, response_class = self._rpc_callbacks[props.correlation_id]
        response = response_class()
        response.ParseFromString(body)

        # Call the callback and remove from the pending requests dictionary.
        # TODO: THINK ABOUT THE THREAD CONTEXT THIS CALLBACK IS BEING CALLED IN!
        callback(props.correlation_id, response)
        del self._rpc_callbacks[props.correlation_id]

    def _on_rpc_request(self, endpoint: RPCEndpoint, _ch, method, props, body):
        message = endpoint.request_class()
        message.ParseFromString(body)

        # Pass RPC request to outer application context, including
        # correlation ID and reply queue, and keeping track of the
        # association between them for later reply processing.
        # TODO: Decide on what to pass here...
        self.out_queue.put(self._wrap(
            Message.rpc(method, props.correlation_id, props.reply_to, message)
        ))

        # Message will be acknowledged when a response is sent.

    def _on_message(self, consumer: Consumer, _ch, method, props, body):
        # All messages are passed as Protocol Buffers messages, so parse the
        # supplied message class from the message body.
        message = consumer.message_class()
        message.ParseFromString(body)

        # Pass the received message to the outer application context,
        # including information about the receivinig exchange and delivery
        # tag.
        self.out_queue.put(self._wrap(
            Message.data(consumer, method, message)
        ))

        # Acknowledge all messages immediately.
        assert self._channel is not None
        self._channel.basic_ack(method.delivery_tag)

    def _send(
            self,
            exchange: str,
            message: pbmsg.Message,
            persistent: bool = True
    ):
        assert self._channel is not None

        if persistent:
            dm = DeliveryMode.Persistent
        else:
            dm = DeliveryMode.Transient

        # Encode Protocol Buffers message and publish.
        self._channel.basic_publish(
            exchange, '', body=message.SerializeToString(),
            properties=spec.BasicProperties(delivery_mode=dm)
        )

    def _send_rpc(
            self,
            endpoint: str,
            payload: pbmsg.Message,
            correlation_id: str
    ):
        # TODO: Think about timeouts.
        # TODO: Think about cancellation.
        # TODO: Think about routing here — a server might want to support more
        #       than one endpoint...
        assert self._rpc_channel is not None
        self._rpc_channel.basic_publish(
            exchange=self.RPC_EXCHANGE,
            routing_key=endpoint,
            properties=BasicProperties(
                reply_to=self.rpc_callback_queue,
                correlation_id=correlation_id
            ),
            body=payload.SerializeToString()
        )

    def _rpc_reply(
            self,
            request_message: Message,
            reply_message: pbmsg.Message
    ):
        # Encode Protocol Buffers message and publish on the RPC exchange
        # using the routing key to send the reply back to the requester.
        assert self._rpc_channel is not None
        self._rpc_channel.basic_publish(
            exchange=self.RPC_EXCHANGE,
            routing_key=request_message.reply_to,
            body=reply_message.SerializeToString(),
            properties=spec.BasicProperties(
                delivery_mode=DeliveryMode.Transient,
                correlation_id=request_message.correlation_id
            )
        )
        # TODO: Get rid of this somehow.
        self._rpc_channel.basic_ack(request_message.delivery_tag)
