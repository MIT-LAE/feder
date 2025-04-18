from datetime import datetime
import logging
from queue import Queue
from threading import Thread, Event

from .config import Config
from .constants import DataSource
from .rmq import RMQ, RPCEndpoint, RPCMessage
from .messages import LivenessQuery, LivenessResponse, Liveness


logger = logging.getLogger(__name__)


class LivenessChecker(Thread):
    @staticmethod
    def endpoint_name(name: str) -> str:
        return 'liveness:' + name

    @staticmethod
    def rpc_endpoints(cfg: Config) -> list[RPCEndpoint]:
        names = ['ingester'] + [str(s) for s in DataSource if cfg.enabled(s)]
        return [
            RPCEndpoint(
                LivenessChecker.endpoint_name(name),
                LivenessQuery,
                LivenessResponse
            )
            for name in names
        ]

    @staticmethod
    def send_reply(
            rmq: RMQ,
            query: RPCMessage,
            status: Liveness = Liveness.OK
    ):
        response = LivenessResponse()
        response.source = rmq.name
        response.timestamp = int(datetime.now().timestamp())
        response.status = status
        rmq.rpc_reply(query, response)

    def __init__(
            self,
            rmq: RMQ,
            name: str,
            out_queue: Queue,
            status_command: type,
            timeout_interval: int = 3,
            ok_check_interval: int = 30,
            down_check_interval: int = 10,
            *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.rmq = rmq
        self.name = name
        self.endpoint = self.endpoint_name(name)
        self.out_queue = out_queue
        self.status_command = status_command
        self.timeout_interval = timeout_interval
        self.ok_check_interval = ok_check_interval
        self.down_check_interval = down_check_interval

        self._stopped = False
        self._correlation_id = None
        self._waiting = None
        self._client_waiting = False
        self._live = False

    @property
    def live(self):
        return self._live

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

    def _callback(self, correlation_id: str, message: LivenessResponse):
        logger.info('liveness response from %s', self.endpoint)
        # TODO: Save last response timestamp and status? More intelligent
        # processing here?
        self._process_callback(correlation_id, True)

    def _error_callback(self, correlation_id: str, reason: str):
        logger.warn('liveness timeout from %s', self.endpoint)
        self._process_callback(correlation_id, False)

    def stop(self):
        self._stopped = True
        if self._waiting is not None:
            self._waiting.set()

    def run(self):
        while not self._stopped:
            self._waiting = Event()
            query = LivenessQuery(source=self.rmq.name)
            self._correlation_id = self.rmq.send_rpc(
                self.endpoint, query,
                self._callback,
                error_callback=self._error_callback,
                timeout=self.timeout_interval
            )
            self._waiting.wait()
            self._waiting = None

            if not self._stopped:
                if not self._client_waiting:
                    self._set_status()
                self._waiting = Event()
                self._waiting.wait(
                    self.ok_check_interval if self._live
                    else self.down_check_interval
                )

    # TODO: Add timeout here?
    def wait(self):
        try:
            self._client_waiting = True
            while not self._stopped and not self._live:
                if self._waiting is not None:
                    self._waiting.wait()
        finally:
            self._client_waiting = False
