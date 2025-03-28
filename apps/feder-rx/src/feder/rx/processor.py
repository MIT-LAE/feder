from datetime import datetime
import logging
from queue import PriorityQueue

from feder.server import Config
from .commands import (
    SourcePositionCommand, SourceErrorCommand, SourceDoneCommand,
    HeartbeatCommand,
    CompleteCommand, TrajectoryCommand,
    StopCommand
)
from .staging import DB


logger = logging.getLogger(__name__)


class Processor:
    def __init__(self, config: Config, source: str, db: DB, queue: PriorityQueue):
        self.config = config
        self.source = source
        self.db = db
        self.queue = queue

    def identify_complete_trajectories(self) -> list[str]:
        horizon = datetime.now()
        horizon -= self.config.data_lag(self.source)
        horizon -= self.config.completion_delay(self.source)
        logger.info('completion horizon: %s', horizon.isoformat())
        return self.db.complete_source_ids(int(horizon.timestamp()))

    def complete_trajectory(self, source_id: str):
        ...

    def run(self):
        done = False
        done_pending = False
        trajectory_completion_pending = False
        trajectory_command_count = 0

        # Process messages from command queue.
        while not done:
            # Only start a new trajectory completion cycle if the last one is
            # complete.
            if trajectory_completion_pending and trajectory_command_count == 0:
                print('Starting trajectory completion cycle...')
                for source_id in self.identify_complete_trajectories():
                    self.queue.put(TrajectoryCommand(source_id))
                    trajectory_command_count += 1
                trajectory_completion_pending = False

            match self.queue.get():
                case SourcePositionCommand():
                    print('SOURCE-POSITION command')
                    # TODO: Handle this.

                case SourceErrorCommand():
                    print('SOURCE-ERROR command')
                    # TODO: Handle this.

                case SourceDoneCommand():
                    print('SOURCE-DONE command')
                    # Run a final trajectory completion cycle and mark that we
                    # should exit when it's finished.
                    trajectory_completion_pending = True
                    done_pending = True

                case StopCommand():
                    # Interrupt: stop immediately!
                    done = True

                case HeartbeatCommand():
                    print('HEARTBEAT command')
                    # TODO: Handle this.

                case CompleteCommand():
                    print('COMPLETE command')
                    # Mark that a trajectory completion cycle should be started
                    # when any current cycle is complete.
                    trajectory_completion_pending = True

                case TrajectoryCommand(source_id):
                    print('TRAJECTORY command')
                    # Process a single complete trajectory.
                    self.complete_trajectory(source_id)

                    # There's one less TRAJECTORY command in the queue.
                    trajectory_command_count -= 1

                    # If we had got a DONE command and were just waiting for
                    # trajectory completion to finish, stop now.
                    if trajectory_command_count == 0 and done_pending:
                        done = True
