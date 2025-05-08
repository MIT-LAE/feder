from collections import defaultdict
from datetime import datetime, timedelta
import logging
from queue import PriorityQueue
from threading import Event
from typing import cast

import psutil

from feder.server import (
    Config, RMQ, LivenessChecker,
    Liveness, LivenessResponse, TrajectoryBatch, log_counts
)
from feder.server.messages import LivenessQuery
import feder.server.rmq as rmq

from .commands import RMQCommand
from .db_cache import DBCache


logger = logging.getLogger(__name__)


class Processor:
    STATISTICS_CLEAN_UP_INTERVAL = timedelta(hours=1)

    def __init__(
            self,
            config: Config,
            db: DBCache,
            queue: PriorityQueue,
            rmq: RMQ
    ):
        self.config = config
        self.db = db
        self.queue = queue
        self.rmq = rmq
        self._trajectory_count = 0
        self._immediate_stop = Event()
        self._rx_trajectory_counts: defaultdict[str, int] = defaultdict(int)
        self._rx_batch_times = {}
        self._last_statistics_cleanup = datetime.now()

    def run(self):
        done = False

        while not done and not self._immediate_stop.is_set():
            if self._statistics_cleanup_due():
                self._clean_up_statistics()

            match self.queue.get():
                case 'STOP':
                    # Used by immediate_stop to break out of loop.
                    continue

                case RMQCommand() as cmd:
                    match cmd.message:
                        case rmq.DataMessage() as msg:
                            batch = cast(TrajectoryBatch, msg.message)
                            self._trajectory_count = log_counts(
                                logger, 'trajectories',
                                self._trajectory_count, len(batch.trajectories), 2
                            )

                            # Batching the DB updates here and only committing
                            # on all the affected connections afterwards looks
                            # a little weird, but is necessary for performance
                            # when processing historical data!
                            dbs_used = set()
                            for traj in batch.trajectories:
                                dbs_used |= self.db.add_trajectory(traj.model)
                            for db in dbs_used:
                                db.commit()

                            # Make sure this is monotonically increasing! If
                            # batches get delivered out of order and we don't
                            # do this, it can confuse the flow control logic
                            # in the receiver.
                            self._rx_trajectory_counts[batch.source] = max(
                                batch.trajectory_count,
                                self._rx_trajectory_counts[batch.source]
                            )
                            self._rx_batch_times[batch.source] = datetime.now()
                        case rmq.RPCMessage() as msg:
                            match msg.endpoint:
                                case 'liveness:ingester':
                                    LivenessChecker.send_reply(
                                        self.rmq, msg,
                                        status=Liveness.OK,
                                        info=self._liveness_info(msg.message)
                                    )
                                case _:
                                    logger.warning(
                                        'unknown RPC endpoint: %s', msg.endpoint
                                    )

    def _liveness_info(self, msg: LivenessQuery) -> LivenessResponse.Info:
        match msg.source:
            case 'monitor':
                # For the monitor, we return all trajectory counts and last
                # batch times for all receivers we know about.
                info = {}
                for s, v in self._rx_trajectory_counts.items():
                    info['trajectory-count:' + s] = v
                for s, v in self._rx_batch_times.items():
                    info['time:' + s] = v
                info['memory_usage'] = psutil.Process().memory_info().rss
                return info

            case _:
                # This request is from the receiver for a single source, so we
                # just send the last ingested count for that source.
                return dict(
                    last_ingested=self._rx_trajectory_counts.get(
                        msg.source, 0
                    )
                )

    def _statistics_cleanup_due(self) -> bool:
        return (
            datetime.now() - self._last_statistics_cleanup >
            self.STATISTICS_CLEAN_UP_INTERVAL
        )

    def _clean_up_statistics(self) -> None:
        to_delete = set()
        now = datetime.now()
        self._last_statistics_cleanup = now
        for s, t in self._rx_batch_times.items():
            if now - t > self.STATISTICS_CLEAN_UP_INTERVAL:
                to_delete.add(s)
        for s in to_delete:
            del self._rx_trajectory_counts[s]
            del self._rx_batch_times[s]

    def immediate_stop(self):
        self._immediate_stop.set()
        if self.queue.empty():
            self.queue.put('STOP')
