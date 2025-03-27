from queue import Queue
from threading import Thread, Event

from feder.server.config import Config

from .commands import HeartbeatCommand, CompleteCommand


class TimerThread(Thread):
    def __init__(self, queue: Queue, interval: int, command: type):
        self.queue = queue
        self.interval = interval
        self.command = command
        self.finished = Event()
        super().__init__()

    def stop(self):
        self.finished.set()

    def run(self):
        while True:
            self.finished.wait(self.interval)
            if self.finished.is_set():
                break
            self.queue.put(self.command())
            self.finished = Event()


class HeartbeatTimerThread(TimerThread):
    def __init__(self, cfg: Config, queue: Queue):
        super().__init__(
            queue, cfg.heartbeat_interval.seconds, HeartbeatCommand
        )


class CompletionTimerThread(TimerThread):
    def __init__(self, cfg: Config, queue: Queue, source: str):
        super().__init__(
            queue, cfg.completion_interval(source).seconds, CompleteCommand
        )
