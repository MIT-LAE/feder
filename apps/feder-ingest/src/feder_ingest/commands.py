from dataclasses import dataclass
from functools import total_ordering

import feder_server.rmq as rmq


# Classes representing different commands that go into the internal command
# queue. These are ordered by priority to support using a PriorityQueue.

@total_ordering
class Command:
    PRIORITY: int = None  # type: ignore

    def __eq__(self, other):
        return self.PRIORITY == other.PRIORITY

    def __lt__(self, other):
        return self.PRIORITY < other.PRIORITY


class CheckpointCommand(Command):
    PRIORITY = 2


@dataclass
class RMQCommand(Command):
    PRIORITY = 1

    message: rmq.Message


class StopCommand(Command):
    PRIORITY = 0
