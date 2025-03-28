from feder.rx.commands import (
    CompleteCommand, HeartbeatCommand,
    SourceDoneCommand, SourceErrorCommand, SourcePositionCommand,
    StopCommand, TrajectoryCommand
)


def test_command_ordering():
    # This is important for the priority queue handling in the receiver.

    source_pos = SourcePositionCommand({'dummy': 'DUMMY'})
    source_error = SourceErrorCommand('this is an error')
    source_done = SourceDoneCommand()
    heartbeat = HeartbeatCommand()
    complete = CompleteCommand()
    trajectory = TrajectoryCommand('dummy-id')
    stop = StopCommand()

    commands = sorted([
        source_pos, source_error, source_done,
        heartbeat, complete, trajectory, stop
    ])

    assert commands == [
        stop,                        # Priority 0
        source_error, heartbeat,     # Priority 1
        source_pos, source_done,     # Priority 2
        complete, trajectory         # Priority 3
    ]
