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
        self._last_batch_time = datetime.now()

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
                            self._last_batch_time = batch.sent_at
                            self._trajectory_count = log_counts(
                                logger, 'trajectories',
                                self._trajectory_count, len(batch.trajectories), 2
                            )
                            for traj in batch.trajectories:
                                self.db.add_trajectory(traj.model)
                        case rmq.RPCMessage() as msg:
                            match msg.endpoint:
                                case 'liveness:ingester':
                                    logger.debug('RPC request: liveness check')
                                    LivenessChecker.send_reply(
                                        self.rmq, msg,
                                        info=dict(
                                            last_batch_time=self._last_batch_time
                                        )
                                    )
                                case _:
                                    logger.warning(
                                        'unknown RPC endpoint: %s', msg.endpoint
                                    )

    def immediate_stop(self):
        self._immediate_stop.set()
        self.queue.put('STOP')
