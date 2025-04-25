import logging
from queue import PriorityQueue
from threading import Event
from typing import cast

from feder.common import Trajectory
from feder.server import Config, RMQ, LivenessChecker, TrajectoryBatch
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
                            old_trajectory_count = self._trajectory_count
                            self._trajectory_count += len(batch.trajectories)
                            if self._trajectory_count // 100 != old_trajectory_count // 100:
                                logger.info('%s trajectories', round(self._trajectory_count, -2))
                            for traj in batch.trajectories:
                                self.db.add_trajectory(traj.model)
                        case rmq.RPCMessage() as msg:
                            match msg.endpoint:
                                case 'liveness:ingester':
                                    logger.debug('RPC request: liveness check')
                                    LivenessChecker.send_reply(self.rmq, msg)
                                case _:
                                    logger.warning(
                                        'unknown RPC endpoint: %s', msg.endpoint
                                    )

    def immediate_stop(self):
        self._immediate_stop.set()
        self.queue.put('STOP')
