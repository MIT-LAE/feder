from datetime import datetime

from feder.rx.commands import (
    FileCompleteCommand, CompleteCommand, IngesterStatusCommand,
    SourceDoneCommand, SourceErrorCommand, SourcePositionCommand,
    StopCommand, TrajectoryCommand
)


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
    source_error = SourceErrorCommand('this is an error')
    source_done = SourceDoneCommand()
    ingester_status = IngesterStatusCommand(live=True)
    complete = CompleteCommand()
    file_complete = FileCompleteCommand()
    trajectory = TrajectoryCommand('dummy-id')
    stop = StopCommand()

    commands = sorted([
        source_pos, source_error, source_done,
        ingester_status, complete, trajectory, stop,
        file_complete
    ])

    assert commands == [
        source_error, stop,          # Priority 0
        ingester_status, trajectory, # Priority 1
        file_complete, source_pos,   # Priority 2
        complete,                    # Priority 3
        source_done                  # Priority 5
    ]
