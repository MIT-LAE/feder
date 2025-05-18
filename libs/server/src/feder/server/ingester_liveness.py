from datetime import datetime, timezone
import logging
from queue import Queue
from threading import Thread, Event
from typing import Any, cast

from .rmq import RMQ, RPCEndpoint, RPCMessage
from .messages import (
    IngesterLivenessQuery, IngesterLivenessResponse, Liveness
)


logger = logging.getLogger(__name__)


class IngesterLivenessChecker(Thread):
    RPC_ENDPOINT_NAME = 'liveness:ingester'

    RPC_ENDPOINTS = [
        RPCEndpoint(
            RPC_ENDPOINT_NAME,
            IngesterLivenessQuery,
            IngesterLivenessResponse
        )
    ]

    @staticmethod
    def send_reply(
            rmq: RMQ,
            query: RPCMessage,
            status: Liveness = Liveness.OK,
            last_ingested: int = 0
    ):
        rmq.rpc_reply(
            query,
            IngesterLivenessResponse(
                source=rmq.name,
                time=datetime.now(timezone.utc),
                status=status,
                last_ingested=last_ingested
            )
        )

    def __init__(
            self,
            rmq: RMQ,
            name: str,
            out_queue: Queue,
            status_command: type,
            timeout_interval: int = 10,
            ok_check_interval: int = 5,
            down_check_interval: int = 1,
            status_extra_kwargs: dict[str, Any] | None = None,
            *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.rmq = rmq
        self.name = name
        self.out_queue = out_queue
        self.status_command = status_command
        self.timeout_interval = timeout_interval
        self.ok_check_interval = ok_check_interval
        self.down_check_interval = down_check_interval
        self.status_extra_kwargs = status_extra_kwargs if status_extra_kwargs is not None else {}

        self._stopped = False
        self._live = False
        self._correlation_id: str | None = None
        self._waiting: Event | None = None
        self._client_waiting = False
        self._last_response_received: datetime | None = None
        self._last_response: IngesterLivenessResponse | None = None

    @property
    def live(self):
        return self._last_response is not None and self._live

    @property
    def last_response_received(self) -> datetime:
        assert self._last_response_received is not None
        return self._last_response_received

    @property
    def last_response_sent(self) -> datetime:
        assert self._last_response is not None
        return self._last_response.time

    def _set_status(self):
        self.out_queue.put(
            self.status_command(
                self._last_response,
                self._last_response_received,
                **self.status_extra_kwargs
            )
        )

    def _callback(self, correlation_id: str, rpc_message: Any):
        logger.debug('liveness response from %s', self.RPC_ENDPOINT_NAME)
        self._last_response = cast(IngesterLivenessResponse, rpc_message)
        self._last_response_received = datetime.now(timezone.utc)
        if (
                not self._stopped and
                self._correlation_id is not None and
                self._correlation_id == correlation_id and
                self._waiting is not None
        ):
            self._correlation_id = None
            self._live = self._last_response.status == Liveness.OK
            self._waiting.set()

    def _error_callback(self, correlation_id: str, reason: str):
        logger.warning('liveness timeout from %s', self.RPC_ENDPOINT_NAME)
        if (
                not self._stopped and
                self._correlation_id is not None and
                self._correlation_id == correlation_id and
                self._waiting is not None
        ):
            self._correlation_id = None
            self._last_response = None
            self._live = False
            self._waiting.set()

    def stop(self):
        self._stopped = True
        if self._waiting is not None:
            self._waiting.set()

    def run(self):
        while not self._stopped:
            self._waiting = Event()
            query = IngesterLivenessQuery(source=self.rmq.name)
            self._correlation_id = self.rmq.send_rpc(
                self.RPC_ENDPOINT_NAME, query,
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

    def wait(self, timeout: float | None = None):
        try:
            self._client_waiting = True
            while not self._stopped and not self._live:
                if self._waiting is not None:
                    self._waiting.wait(timeout)
        finally:
            self._client_waiting = False
