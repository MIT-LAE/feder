from dataclasses import dataclass
from functools import total_ordering


# Classes representing different commands that go into the internal command
# queue. These are ordered by priority to support using a PriorityQueue.

@total_ordering
class Command:
    PRIORITY = None

    def __eq__(self, other):
        return self.PRIORITY == other.PRIORITY

    def __lt__(self, other):
        return self.PRIORITY < other.PRIORITY


@dataclass
class SourcePositionCommand(Command):
    PRIORITY = 2

    # TODO: Refine this.
    data: dict[str, str]


@dataclass
class SourceErrorCommand(Command):
    PRIORITY = 1

    message: str


class SourceDoneCommand(Command):
    PRIORITY = 2


class HeartbeatCommand(Command):
    PRIORITY = 1


class CompleteCommand(Command):
    PRIORITY = 3


@dataclass
class TrajectoryCommand(Command):
    PRIORITY = 3

    source_id: str


class StopCommand(Command):
    PRIORITY = 0
