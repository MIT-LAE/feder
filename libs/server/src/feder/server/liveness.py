from datetime import datetime
from queue import Queue
from threading import Thread, Event

from google.protobuf.message import Message

from .rmq import RMQ, RPCEndpoint, RPCMessage
from .liveness_pb2 import LivenessQuery, LivenessResponse, LivenessStatus


class LivenessChecker(Thread):
    @staticmethod
    def rpc_endpoints(*endpoint_names):
        return [
            RPCEndpoint(name, LivenessQuery, LivenessStatus)
            for name in endpoint_names
        ]

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

        self._stopped = False
        self._correlation_id = None
        self._waiting = None
        self._live = False

    def _set_status(self):
        self.out_queue.put(self.status_command(self._live))

    def _process_callback(self, correlation_id: str, status: bool):
        if (
                not self._stopped and
                self._correlation_id is not None and
                self._correlation_id == correlation_id and
                self._waiting is not None
        ):
            self._correlation_id = None
            self._live = status
            self._waiting.set()

    def _callback(self, correlation_id: str, message: Message):
        self._process_callback(correlation_id, True)

    def _error_callback(self, correlation_id: str, reason: str):
        self._process_callback(correlation_id, False)

    def stop(self):
        self._stopped = True
        if self._waiting is not None:
            self._waiting.is_set()

    def run(self):
        while not self._stopped:
            self._waiting = Event()
            query = LivenessQuery()
            query.source = self.rmq.name
            self._correlation_id = self.rmq.send_rpc(
                self.endpoint, query,
                self._callback,
                error_callback=self._error_callback,
                timeout=self.timeout_interval
            )
            self._waiting.wait()
            self._waiting = None

            if not self.stopped:
                self._set_status()
                self._waiting = Event()
                self._waiting.wait(
                    self.ok_check_interval if self._live
                    else self.down_check_interval
                )

    def send_reply(
            self, query: RPCMessage, status: LivenessStatus = LivenessStatus.OK
    ):
        response = LivenessResponse()
        response.source = self.rmq.name
        response.timestamp = datetime.now().timestamp()
        response.status = self._live
        self.rmq.rpc_reply(query, response)
