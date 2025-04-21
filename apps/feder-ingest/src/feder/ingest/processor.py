import logging
from queue import Queue
from typing import cast

from feder.common import Trajectory
from feder.server import Config, RMQ, LivenessChecker
import feder.server.rmq as rmq

from .commands import StopCommand, RMQCommand
from .db_cache import DBCache


logger = logging.getLogger(__name__)


class Processor:
    def __init__(
            self,
            config: Config,
            db: DBCache,
            queue: Queue,
            rmq: RMQ
    ):
        self.config = config
        self.db = db
        self.queue = queue
        self.rmq = rmq

    def run(self):
        done = False

        while not done:
            match self.queue.get():
                case StopCommand():
                    # Interrupt: stop immediately!
                    logger.info('STOP command')
                    done = True

                case RMQCommand() as cmd:
                    match cmd.message:
                        case rmq.DataMessage() as msg:
                            trajectory = cast(Trajectory, msg.message)
                            logger.info('TRAJECTORY message: %s', trajectory.model.callsign)
                            self.db.add_trajectory(trajectory.model)
                        case rmq.RPCMessage() as msg:
                            match msg.endpoint:
                                case 'liveness:ingester':
                                    logger.debug('RPC request: liveness check')
                                    LivenessChecker.send_reply(self.rmq, msg)
                                case _:
                                    logger.warning(
                                        'unknown RPC endpoint: %s', msg.endpoint
                                    )
                        case rmq.DataMessage() as msg:
                            logger.info(
                                'trajectory for callsign %s', msg.data.callsign
                            )
