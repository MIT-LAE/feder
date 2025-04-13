from queue import Queue
from threading import Thread, Event

from google.protobuf.message import Message

from .rmq import RMQ


class LivenessChecker(Thread):
    def __init__(
            self,
            rmq: RMQ,
            endpoint: str,
            out_queue: Queue,
            status_command: type,
            timeout_interval: int = 3,
            ok_check_interval: int = 30,
            down_check_interval: int = 10
    ):
        self.rmq = rmq
        self.endpoint = endpoint
        self.out_queue = out_queue
        self.status_command = status_command
        self.timeout_interval = timeout_interval
        self.ok_check_interval = ok_check_interval
        self.down_check_interval = down_check_interval
        self.stopped = False
        self.correlation_id = None
        self.waiting = None
        self.live = False

    def _set_status(self):
        self.out_queue.put(self.status_command(self.live))

    def _callback(self, correlation_id: int, message: Message):
        if (
                not self.stopped and
                self.correlation_id is not None and
                self.correlation_id == correlation_id and
                self.waiting is not None
        ):
            self.correlation_id = None
            self.waiting.set()

    def stop(self):
        self.stopped = True
        if self.waiting is not None:
            self.waiting.is_set()

    def run(self):
        while not self.stopped:
            self.waiting = Event()
            self.correlation_id = self.rmq.send_rpc(self.endpoint, self._callback)
            self.waiting.wait(self.timeout_interval)
            self.live = self.waiting.is_set()
            self.waiting = None
            self._set_status()

            if not self.stopped:
                self.waiting = Event()
                self.waiting.wait(
                    self.ok_check_interval if self.live
                    else self.down_check_interval
                )
