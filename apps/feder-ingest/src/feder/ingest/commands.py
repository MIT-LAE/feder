from dataclasses import dataclass
from datetime import datetime
from functools import total_ordering

import feder.server.rmq as rmq


# Classes representing different commands that go into the internal command
# queue. These are ordered by priority to support using a PriorityQueue.

@total_ordering
class Command:
    PRIORITY = None

    def __eq__(self, other):
        return self.PRIORITY == other.PRIORITY

    def __lt__(self, other):
        return self.PRIORITY < other.PRIORITY


class StopCommand(Command):
    PRIORITY = 0


@dataclass
class RMQCommand(Command):
    PRIORITY = 1

    message: rmq.Message
