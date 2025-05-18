from datetime import datetime, timezone
import json
import logging
from queue import PriorityQueue
from threading import Event

from feder.server import Config

from .commands import StopCommand, TriggerCommand, StatusCommand


logger = logging.getLogger(__name__)


class Processor:
    def __init__(
            self,
            config: Config,
            command_queue: PriorityQueue,
            status_sources: list[str]
    ):
        self.config = config
        self.command_queue = command_queue
        self.status_sources = status_sources

        self._done = False
        self._statuses = {}
        self._last_updates = {}
        self._immediate_stop = Event()

    def run(self):
        while not self._done:
           try:
               self._process_one(self.command_queue.get())
           except Exception:
               logger.exception('command processing failed')

    def immediate_stop(self):
        self._immediate_stop.set()
        if self.command_queue.empty():
            self.command_queue.put(StopCommand())

    def _process_one(self, command):
        match command:
            case StopCommand():
                self._done = True

            case TriggerCommand():
                self.update()

            case StatusCommand() as cmd:
                self._statuses[cmd.source] = cmd.info
                self._last_updates[cmd.source] = datetime.now(timezone.utc)

    def update(self):
        # Update stored data from current status information.
        update_times = {
            s: self._last_updates.get(s) for s in self.status_sources
        }
        with open('current-status.json', 'w', encoding='utf-8') as fp:
            json.dump(update_times, fp)
