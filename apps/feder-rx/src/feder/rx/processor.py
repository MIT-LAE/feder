from datetime import datetime
import logging
from queue import PriorityQueue

from feder.server import Config
from feder.server.messaging import TrajectoryMessage
from .commands import (
    SourcePositionCommand, SourceErrorCommand, SourceDoneCommand,
    HeartbeatCommand,
    CompleteCommand, TrajectoryCommand,
    StopCommand
)
from .db import DB


logger = logging.getLogger(__name__)


class Processor:
    def __init__(
            self,
            config: Config, source: str, historical: bool,
            db: DB, queue: PriorityQueue):
        self.config = config
        self.source = source
        self.historical = historical
        self.db = db
        self.queue = queue
        self.data_lag = self.config.data_lag(self.source)
        self.completion_delay = self.config.completion_delay(self.source)
        # Used for historical processing.
        self.horizon_reference = None

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
                case SourcePositionCommand() as cmd:
                    print('SOURCE-POSITION command')
                    if self.historical:
                        self.horizon_reference = cmd.time
                    self.db.save_position(
                        cmd.source_id, cmd.transponder_id, cmd.time,
                        cmd.callsign, cmd.aircrafttype,
                        cmd.lat, cmd.lon, cmd.alt, cmd.alt_gnss,
                        cmd.heading, cmd.on_ground
                    )

                case SourceErrorCommand(message):
                    # We just log the errors here. If a source wants to exit
                    # after an error, it will send a StopCommand.
                    print('SOURCE-ERROR command')
                    logger.error(message)

                case SourceDoneCommand():
                    print('SOURCE-DONE command')
                    # Run a final trajectory completion cycle and mark that we
                    # should exit when it's finished.
                    trajectory_completion_pending = True
                    done_pending = True

                    # Ensure that the rejectory completion horizon is far
                    # enough past the time of the last position fix received
                    # so that all data in the staging database is consumed
                    # during the final trajectory completion cycle.
                    if self.historical and self.horizon_reference is not None:
                        self.horizon_reference += self.completion_delay

                case StopCommand():
                    # Interrupt: stop immediately!
                    print('STOP command')
                    done = True

                case HeartbeatCommand():
                    print('HEARTBEAT command')
                    self.send_heartbeat()

                case CompleteCommand():
                    print('COMPLETE command')
                    # Mark that a trajectory completion cycle should be started
                    # when any current cycle is complete.
                    trajectory_completion_pending = True

                case TrajectoryCommand(source_id):
                    print('TRAJECTORY command')
                    # Process a single complete trajectory. If this doesn't
                    # work, then the position fixes for the trajectory will
                    # remain in the database to be reprocessed in the next
                    # completion cycle.
                    #
                    # A special case here is if a trajectory completion fails
                    # in the final trajectory completion cycle of a historical
                    # processing job. (This case just falls through to the
                    # "attempt to remove a non-empty staging database" error,
                    # since there will be position fixes left in the database
                    # when the process exits.)
                    self.complete_trajectory(source_id)

                    # There's one less TRAJECTORY command in the queue.
                    trajectory_command_count -= 1

                    # If we had got a DONE command and were just waiting for
                    # trajectory completion to finish, stop now.
                    if trajectory_command_count == 0 and done_pending:
                        done = True

    def identify_complete_trajectories(self) -> list[str]:
        # For historical processing jobs, we use the time of the last position
        # fix as a reference time for calculating the trajectory completion
        # horizon. For live processing, we use the current time (minus any
        # source-dependent data lag) as the reference. (For the final
        # trajectory completion cycle of a historical processing job, the
        # horizon reference is set far enough past the final position fix
        # received to ensure that all position data is processed and that the
        # staging database is empty when the job completes).
        if self.historical:
            if self.horizon_reference is None:
                # The case where a completion trajectory is triggered for a
                # historical processing job before we have any position fixes.
                return []
            horizon = self.horizon_reference
        else:
            horizon = datetime.now() - self.data_lag
        horizon -= self.completion_delay
        logger.info('completion horizon: %s', horizon.isoformat())
        return self.db.complete_source_ids(horizon)

    def complete_trajectory(self, source_id: str):
        # Retrieve all position fixes from database as data frame.
        df = self.db.get_trajectory(source_id)

        # Build trajectory payload to send to ingester.
        payload = TrajectoryMessage(self.source, source_id, df)

        # Send trajectory payload out over RabbitMQ.

        # If successful, delete all position fixes from the database.
        ...

    def send_heartbeat(self):
        # Collect statistics from source and database for heartbeat message.

        # Construct heartbeat payload to send to monitor.

        # Send heartbeat payload out over RabbitMQ.
        ...
