from dataclasses import dataclass
from functools import total_ordering

import feder.server.rmq as rmq
from feder.server.messages import LivenessResponse


# Classes representing different commands that go into the internal command
# queue. These are ordered by priority to support using a PriorityQueue.

@total_ordering
class Command:
    def priority(self) -> rmq.Message.Priority:
        return rmq.Message.Priority.MEDIUM

    def __eq__(self, other):
        return self.priority() == other.priority()

    def __lt__(self, other):
        return self.priority() < other.priority()


class StopCommand(Command):
    def priority(self) -> rmq.Message.Priority:
        return rmq.Message.Priority.MAXIMUM


class TriggerCommand(Command):
    pass


@dataclass
class StatusCommand(Command):
    live: bool
    info: LivenessResponse.Info
    source: str

    def priority(self) -> rmq.Message.Priority:
        return rmq.Message.Priority.HIGH
