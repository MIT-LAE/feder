from datetime import datetime

from feder.rx.commands import (
    IngesterStatusCommand,
    SourceDoneCommand, SourceErrorCommand, SourcePositionCommand,
    RMQCommand
)
import feder.server.rmq as rmq


def test_command_ordering():
    # This is important for the priority queue handling in the receiver.

    source_pos = SourcePositionCommand(
        source_id='DUMMY', transponder_id='DUMMY',
        time=datetime(2025, 4, 1, 12, 0),
        orig=None, dest='DUMY',
        callsign='DUMMY', aircraft_type=None,
        lat=41.0, lon=-95.0, alt=35000, alt_gnss=None, heading=None,
        on_ground=False
    )
    source_error = SourceErrorCommand('this is an error', stop=True)
    source_done = SourceDoneCommand(datetime.now())
    ingester_status = IngesterStatusCommand(live=True)
    rmq_ack = RMQCommand(message=rmq.AckMessage(delivery_tag=2))
    rmq_nack = RMQCommand(message=rmq.NackMessage(delivery_tag=1))
    rmq_data = RMQCommand(message=rmq.DataMessage(
        delivery_tag=4, exchange='test-exchange', message='DUMMY'
    ))
    rmq_rpc = RMQCommand(message=rmq.RPCMessage(
        delivery_tag=5,
        endpoint='test-endpoint',
        reply_to='amq.something',
        correlation_id='DUMMY-ID',
        message='DUMMY'
    ))
    rmq_rpc_error = RMQCommand(message=rmq.RPCErrorMessage(
        delivery_tag=6,
        endpoint='test-endpoint',
        correlation_id='DUMMY-ID',
        reason='testing'
    ))

    commands = sorted([
        source_pos, source_error, source_done,
        ingester_status,
        rmq_ack, rmq_nack, rmq_data, rmq_rpc, rmq_rpc_error
    ])

    assert commands == [
        # High priority
        source_error, ingester_status,
        rmq_nack, rmq_ack, rmq_rpc_error,

        # Medium priority
        source_pos, rmq_rpc,

        # Low priority
        source_done, rmq_data
    ]
