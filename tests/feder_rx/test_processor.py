from dataclasses import dataclass
from datetime import datetime
from queue import PriorityQueue
from threading import Thread
from unittest.mock import Mock

from feder.server.rmq import Message
from feder.rx import Processor
from feder.rx.commands import SourceDoneCommand, SourcePositionCommand
from feder.rx.sources.flightaware import FlightAwareSource


@dataclass
class Wrapper:
    content: Message


def test_source_position_command_processing(config, db):
    # Test that source position commands result in new position fix rows in
    # the database.

    queue = PriorityQueue()
    processor = Processor(
        config, FlightAwareSource.NAME, False, db, queue,
        rmq=Mock()
    )

    def send_position_fixes():
        queue.put(SourcePositionCommand(
            source_id='DUMMY-001', transponder_id='DUMMY',
            time=datetime(2025, 4, 1, 12, 0), callsign='DUMMY', aircrafttype=None,
            lat=41.0, lon=-95.0, alt=35000, alt_gnss=None, heading=None,
            on_ground=False
        ))
        queue.put(SourcePositionCommand(
            source_id='DUMMY-001', transponder_id='DUMMY',
            time=datetime(2025, 4, 1, 12, 1), callsign='DUMMY', aircrafttype=None,
            lat=41.1, lon=-95.0, alt=35000, alt_gnss=None, heading=None,
            on_ground=False
        ))
        queue.put(SourcePositionCommand(
            source_id='DUMMY-001', transponder_id='DUMMY',
            time=datetime(2025, 4, 1, 12, 2), callsign='DUMMY', aircrafttype=None,
            lat=41.2, lon=-95.0, alt=35000, alt_gnss=None, heading=None,
            on_ground=False
        ))
        queue.put(SourceDoneCommand())

    before_rows = db.count_entries()

    adder = Thread(target=send_position_fixes)
    adder.start()
    processor.run()
    adder.join()

    after_rows = db.count_entries()

    assert after_rows - before_rows == 3
