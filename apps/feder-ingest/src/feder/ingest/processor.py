from collections import defaultdict
from datetime import datetime
import logging
from queue import PriorityQueue
from threading import Event
from typing import cast

from feder.server import (
    Config, RMQ, LivenessChecker, TrajectoryBatch, log_counts
)
import feder.server.rmq as rmq

from .commands import RMQCommand
from .db_cache import DBCache


logger = logging.getLogger(__name__)


class Processor:
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
        # TODO: Add timer to clean these up — once per hour, delete any
        # entries for which time is more than an hour in the past.
        self._batch_trajectory_counts = defaultdict(int)
        self._process_count_times = {}

    def run(self):
        done = False

        while not done and not self._immediate_stop.is_set():
            match self.queue.get():
                case 'STOP':
                    # Used by immediate_stop to break out of loop.
                    pass

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
                            self._batch_trajectory_counts[batch.source] = max(
                                batch.trajectory_count,
                                self._batch_trajectory_counts[batch.source]
                            )
                            self._process_count_times[batch.source] = datetime.now()
                        case rmq.RPCMessage() as msg:
                            match msg.endpoint:
                                case 'liveness:ingester':
                                    # logger.info(
                                    #     'RPC request: liveness check: %s',
                                    #     msg.message.source
                                    # )
                                    LivenessChecker.send_reply(
                                        self.rmq, msg,
                                        info=dict(
                                            last_ingested=self._batch_trajectory_counts.get(
                                                msg.message.source, 0
                                            )
                                        )
                                    )
                                case _:
                                    logger.warning(
                                        'unknown RPC endpoint: %s', msg.endpoint
                                    )

    def immediate_stop(self):
        self._immediate_stop.set()
        self.queue.put('STOP')
