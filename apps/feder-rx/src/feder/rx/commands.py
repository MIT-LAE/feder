# Classes representing different commands that go into the internal command
# queue.

class Command:
    ...


class SourcePositionCommand(Command):
    ...


class SourceErrorCommand(Command):
    ...


class SourceDoneCommand(Command):
    ...


class HeartbeatCommand(Command):
    ...


class CompleteCommand(Command):
    ...


class TrajectoryCommand(Command):
    ...


class CleanCommand(Command):
    ...


class StopCommand(Command):
    ...
