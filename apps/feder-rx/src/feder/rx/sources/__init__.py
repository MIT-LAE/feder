import os
from queue import PriorityQueue
from threading import Thread
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from feder.server import Config


class Source(Thread):
    NAME = None

    def __init__(self, config: 'Config', queue: PriorityQueue, *files: str):
        super().__init__()
        self.config = config
        self.queue = queue
        self.files = files
        if self.NAME is None:
            raise ValueError('unknown source name')

    def run(self):
        ...
