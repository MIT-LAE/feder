from datetime import datetime, timezone

from feder_rx.commands import (
    StopCommand,
    SourceErrorCommand, SourceDoneCommand,
    SourcePositionCommand, BatchSourcePositionCommand,
    IngesterStatusCommand, EndOfDayCommand,
    RMQCommand
)
import feder_server.rmq as rmq


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
    batch_source_pos = BatchSourcePositionCommand(
        source_ids=['DUMMY'], transponder_ids=['DUMMY'],
        times=[datetime(2025, 4, 1, 12, 0)],
        origs=[None], dests=['DUMY'],
        callsigns=['DUMMY'], aircraft_types=[None],
        lats=[41.0], lons=[-95.0], alts=[35000], alts_gnss=[None], headings=[None],
        on_grounds=[False]
    )
    source_error = SourceErrorCommand('this is an error', stop=True)
    source_done = SourceDoneCommand(datetime.now(timezone.utc))
    stop = StopCommand()
    ingester_status = IngesterStatusCommand(
        response=None,
        response_received=datetime.now(timezone.utc)
    )
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
    end_of_day = EndOfDayCommand(datetime.now(timezone.utc))

    commands = sorted([
        source_pos, batch_source_pos,
        source_error, source_done, stop,
        ingester_status, end_of_day,
        rmq_ack, rmq_nack, rmq_data, rmq_rpc, rmq_rpc_error
    ])

    assert commands == [
        # Maximum priority
        stop,

        # High priority
        source_error, ingester_status,
        rmq_nack, rmq_ack, rmq_rpc_error, rmq_rpc, rmq_data,

        # Medium priority
        source_pos, batch_source_pos, end_of_day,

        # Low priority
        source_done
    ]
