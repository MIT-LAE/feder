from queue import Queue
from threading import Thread, Event


class TimerThread(Thread):
    def __init__(self, queue: Queue, interval: int, command):
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
