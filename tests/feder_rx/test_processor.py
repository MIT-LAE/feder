from dataclasses import dataclass
from datetime import datetime
from queue import PriorityQueue
from threading import Thread
import time
from unittest.mock import Mock

from feder_common import DataSource
from feder_server import Message
import feder_server.rmq as rmq
from feder_rx import Processor
from feder_rx.db import DB
from feder_rx.commands import (
    SourceDoneCommand, SourcePositionCommand, RMQCommand
)


@dataclass
class Wrapper:
    content: Message


def test_source_position_command_processing(config):
    # Test that source position commands result in new position fix rows in
    # the database and that these get processed into a trajectory.

    db = DB(config, 'processor')
    queue = PriorityQueue()
    rmq_mock = Mock()
    thread_control_mock = Mock()
    DELIVERY_TAG = 123
    rmq_mock.send = Mock(return_value=DELIVERY_TAG)
    processor = Processor(
        config, DataSource.FLIGHTAWARE, 'test', False,
        db, queue, rmq_mock, thread_control_mock,
        ingester_liveness_interval=10
    )

    def send_position_fixes():
        queue.put(SourcePositionCommand(
            source_id='DUMMY-001', transponder_id='DUMMY',
            time=datetime(2025, 4, 1, 12, 0),
            orig='DUMA', dest='DUMZ',
            callsign='DUMMY', aircraft_type=None,
            lat=41.0, lon=-95.0, alt=35000, alt_gnss=None, heading=None,
            on_ground=False
        ))
        queue.put(SourcePositionCommand(
            source_id='DUMMY-001', transponder_id='DUMMY',
            time=datetime(2025, 4, 1, 12, 1),
            orig='DUMA', dest='DUMZ',
            callsign='DUMMY', aircraft_type=None,
            lat=41.1, lon=-95.0, alt=35000, alt_gnss=None, heading=None,
            on_ground=False
        ))
        queue.put(SourcePositionCommand(
            source_id='DUMMY-001', transponder_id='DUMMY',
            time=datetime(2025, 4, 1, 12, 2),
            orig='DUMA', dest='DUMZ',
            callsign='DUMMY', aircraft_type=None,
            lat=41.2, lon=-95.0, alt=35000, alt_gnss=None, heading=None,
            on_ground=False
        ))
        queue.put(SourceDoneCommand(latest_time=datetime(2025, 4, 1, 12, 2)))
        time.sleep(0.01)
        queue.put(RMQCommand(
            message=rmq.AckMessage(delivery_tag=DELIVERY_TAG)
        ))

    before_rows = db.count_entries()

    adder = Thread(target=send_position_fixes)
    adder.start()
    processor.run()
    adder.join()

    after_rows = db.count_entries()

    assert after_rows - before_rows == 0
    assert rmq_mock.send.call_count == 1
