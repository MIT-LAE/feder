from collections import defaultdict
from datetime import datetime, timedelta, timezone
import logging
from queue import PriorityQueue
from threading import Event
from typing import cast

from feder.server import (
    Config, RMQ, IngesterLivenessChecker,
    Liveness, TrajectoryBatch, log_counts
)
import feder.server.rmq as rmq

from .commands import RMQCommand
from .db_cache import DBCache
from .monitoring import trajectory_counter, batch_time_gauge


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
        self._last_statistics_cleanup = datetime.now(timezone.utc)

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

                            # Make sure the trajectory count is monotonically
                            # increasing! If batches get delivered out of
                            # order and we don't do this, it can confuse the
                            # flow control logic in the receiver.
                            old_count = self._rx_trajectory_counts[batch.source]
                            new_count = max(batch.trajectory_count, old_count)
                            self._rx_trajectory_counts[batch.source] = new_count
                            self._rx_batch_times[batch.source] = datetime.now(timezone.utc)

                            # Update monitoring metrics.
                            trajectory_counter.labels(source=batch.source).inc(new_count - old_count)
                            batch_time_gauge.labels(source=batch.source).set_to_current_time()

                        case rmq.RPCMessage() as msg:
                            match msg.endpoint:
                                case 'liveness:ingester':
                                    # This request is from a receiver so we
                                    # send the last ingested count for that
                                    # source as extra information.
                                    IngesterLivenessChecker.send_reply(
                                        self.rmq, msg,
                                        status=Liveness.OK,
                                        last_ingested=self._rx_trajectory_counts.get(
                                            msg.message.source, 0
                                        )
                                    )
                                case _:
                                    logger.warning(
                                        'unknown RPC endpoint: %s', msg.endpoint
                                    )

    def _statistics_cleanup_due(self) -> bool:
        return (
            datetime.now(timezone.utc) - self._last_statistics_cleanup >
            self.STATISTICS_CLEAN_UP_INTERVAL
        )

    def _clean_up_statistics(self) -> None:
        to_delete = set()
        now = datetime.now(timezone.utc)
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
