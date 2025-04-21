from datetime import datetime
import logging
from queue import PriorityQueue

from feder.common import DataSource
from feder.server import Config, RMQ, Trajectory, LivenessChecker
import feder.server.rmq as rmq

from .commands import (
    SourcePositionCommand, BatchSourcePositionCommand,
    SourceErrorCommand, SourceDoneCommand,
    IngesterStatusCommand,
    CompleteCommand, FileCompleteCommand, TrajectoryCommand,
    StopCommand, RMQCommand
)
from .db import DB


logger = logging.getLogger(__name__)


class Processor:
    def __init__(
            self,
            config: Config, source: DataSource, historical: bool,
            db: DB, queue: PriorityQueue, rmq: RMQ,
            liveness_endpoint: str | None
    ):
        self.config = config
        self.source = source
        self.historical = historical
        self.db = db
        self.queue = queue
        self.rmq = rmq
        self.data_lag = self.config.data_lag(self.source)
        self.completion_delay = self.config.completion_delay(self.source)
        self.liveness_endpoint = liveness_endpoint
        # Used for historical processing.
        self.horizon_reference = None

        self._done = False
        self._done_pending = False
        self._trajectory_completion_pending = False
        self._trajectory_command_count = 0
        self._pending_rmq_messages = {}
        self._fix_count = 0
        self._trajectory_count = 0
        self._unique_trajectories = set()

    def run(self):
        # Process messages from command queue.
        while not self._done:
            # Only start a new trajectory completion cycle if the last one is
            # complete.
            if (
                    self._trajectory_completion_pending and
                    self._trajectory_command_count == 0
            ):
                logger.info('Starting trajectory completion cycle...')
                for source_id in self._identify_complete_trajectories():
                    self.queue.put(TrajectoryCommand(source_id))
                    self._trajectory_command_count += 1
                logger.info(
                    'Queued trajectories: %s (%s)',
                    self._trajectory_command_count, self.queue.qsize()
                )
                self._trajectory_completion_pending = False

            command = self.queue.get()
            try:
                self._process_one(command)
            except Exception:
                logger.exception('command processing failed')

            # If we had got a DONE command and were just waiting for
            # trajectory completion to finish, stop now. This has to be called
            # in the right place!
            if (
                    self._trajectory_command_count == 0 and
                    self._done_pending and
                    len(self._pending_rmq_messages) == 0
            ):
                self._done = True

    def _process_one(self, command):
        match command:
            case SourcePositionCommand() as cmd:
                self._source_position(cmd)

            case BatchSourcePositionCommand() as cmd:
                self._batch_source_position(cmd)

            case SourceErrorCommand(message):
                # We just log the errors here. If a source wants to exit
                # after an error, it will send a StopCommand.
                logger.info('SOURCE-ERROR command')
                logger.error(message)

            case SourceDoneCommand():
                self._source_done()

            case StopCommand():
                # Interrupt: stop immediately!
                logger.info('STOP command')
                self._done = True

            case IngesterStatusCommand(live):
                logger.info(
                    'INGESTER-STATUS command: %s',
                    'OK' if live else 'FAILED'
                )
                # TODO: Handle changes in ingester status here.

            case CompleteCommand():
                logger.info('COMPLETE command')
                # Mark that a trajectory completion cycle should be started
                # when any current cycle is complete.
                self._trajectory_completion_pending = True

            case FileCompleteCommand():
                logger.info('FILE-COMPLETE command')
                # Mark that a trajectory completion cycle should be started
                # when any current cycle is complete.
                self._trajectory_completion_pending = True

            case TrajectoryCommand(source_id):
                self._trajectory(source_id)

            case RMQCommand() as cmd:
                match cmd.message:
                    case rmq.AckMessage(delivery_tag):
                        # If publication to RabbitMQ was successful, delete all
                        # position fixes from the database for the related source
                        # ID.
                        if delivery_tag in self._pending_rmq_messages:
                            source_id = self._pending_rmq_messages[delivery_tag]
                            self.db.delete_trajectory(source_id)
                            del self._pending_rmq_messages[delivery_tag]

                    case rmq.NackMessage(delivery_tag):
                        # If publication to RabbitMQ was unsuccessful, don't
                        # delete position fixes from the database for the related
                        # source ID. They will be picked up again in the next
                        # trajectory completion cycle.
                        if delivery_tag in self._pending_rmq_messages:
                            source_id = self._pending_rmq_messages[delivery_tag]
                            del self._pending_rmq_messages[delivery_tag]

                    case rmq.RPCMessage() as msg:
                        if msg.endpoint == self.liveness_endpoint:
                            logger.debug('RPC request: liveness check')
                            LivenessChecker.send_reply(self.rmq, msg)
                        else:
                            logger.warning(
                                'unknown RPC endpoint: %s', msg.endpoint
                            )

                    case _:
                        logger.warning(
                            'unexpected RMQ message "%s"', cmd.message
                        )

    def _source_position(self, cmd):
        if self.historical:
            self.horizon_reference = cmd.time
        self.db.save_position(
            cmd.source_id, cmd.transponder_id, cmd.time,
            cmd.orig, cmd.dest, cmd.callsign, cmd.aircraft_type,
            cmd.lat, cmd.lon, cmd.alt, cmd.alt_gnss,
            cmd.heading, cmd.on_ground
        )
        self._fix_count += 1
        if self._fix_count % 1000 == 0:
            logger.info(
                'processed %s position fixes', self._fix_count
            )

    def _batch_source_position(self, cmd):
        if self.historical:
            self.horizon_reference = cmd.times[-1]
        self.db.save_positions(
            cmd.source_ids, cmd.transponder_ids, cmd.times,
            cmd.origs, cmd.dests, cmd.callsigns, cmd.aircraft_types,
            cmd.lats, cmd.lons, cmd.alts, cmd.alts_gnss,
            cmd.headings, cmd.on_grounds
        )
        for i in range(len(cmd.times)):
            self._fix_count += 1
            if self._fix_count % 1000 == 0:
                logger.info(
                    'processed %s position fixes', self._fix_count
                )

    def _source_done(self):
        logger.info('SOURCE-DONE command')
        # Run a final trajectory completion cycle and mark that we
        # should exit when it's finished.
        self._trajectory_completion_pending = True
        self._done_pending = True

        # Ensure that the rejectory completion horizon is far
        # enough past the time of the last position fix received
        # so that all data in the staging database is consumed
        # during the final trajectory completion cycle.
        if self.historical and self.horizon_reference is not None:
            self.horizon_reference += self.completion_delay

    def _trajectory(self, source_id: str):
        self._trajectory_count += 1
        self._unique_trajectories.add(source_id)
        logger.info(
            'TRAJECTORY %s (%s): %s',
            self._trajectory_count,
            len(self._unique_trajectories),
            source_id
        )

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
        #
        # The message number from RabbitMQ is saved for publish
        # confirmation processing.
        message_number = self._complete_trajectory(source_id)
        if message_number is not None:
            self._pending_rmq_messages[message_number] = source_id

        # There's one less TRAJECTORY command in the queue.
        self._trajectory_command_count -= 1

    def _identify_complete_trajectories(self) -> list[str]:
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

    def _complete_trajectory(self, source_id: str) -> int | None:
        # Retrieve all position fixes from database as data frame.
        df = self.db.get_trajectory(source_id)
        if df.empty:
            return None

        # Build trajectory payload to send to ingester.
        payload = Trajectory.build(self.source, source_id, df)

        # Send trajectory payload out over RabbitMQ, returning message number
        # for ACK/NACK processing.
        return self.rmq.send('trajectory', payload)
