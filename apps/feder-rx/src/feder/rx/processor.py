from datetime import datetime, timedelta
import logging
from queue import Queue

from feder.common import DataSource
from feder.server import Config, RMQ, Trajectory, LivenessChecker
import feder.server.rmq as rmq

from .commands import (
    SourcePositionCommand, BatchSourcePositionCommand,
    SourceErrorCommand, SourceDoneCommand,
    IngesterStatusCommand, CompleteCommand,
    StopCommand, RMQCommand
)
from .db import DB


logger = logging.getLogger(__name__)


class Processor:
    N_FIXES_FOR_COMPLETION = 500
    DT_LATEST_FOR_COMPLETION = timedelta(minutes=5)
    DT_REAL_FOR_COMPLETIONS = timedelta(minutes=15)

    def __init__(
            self,
            config: Config, source: DataSource, historical: bool,
            # db: DB, queue: PriorityQueue, rmq: RMQ,
            db: DB, command_queue: Queue,
            rmq: RMQ, liveness_endpoint: str | None
    ):
        self.config = config
        self.source = source
        self.historical = historical
        self.db = db
        self.command_queue = command_queue
        self.rmq = rmq
        self.data_lag = self.config.data_lag(self.source)
        self.completion_delay = self.config.completion_delay(self.source)
        self.liveness_endpoint = liveness_endpoint

        self._trajectory_queue = Queue()
        self._done = False
        self._done_pending = False
        self._final_completion_pending = False
        self._pending_rmq_messages = {}
        self._fix_count_total = 0
        self._fix_count_last_completion = 0
        self._fix_time_latest = None
        self._fix_time_last_completion = None
        self._real_time_last_completion = datetime.now()
        self._trajectory_count = 0

    def run(self):
        # Process messages from command queue.
        while not self._done:
            # If we had got a DONE command and were just waiting for
            # trajectory completion to finish, stop now.
            if (
                    self._done_pending and
                    not self._final_completion_pending and
                    self.command_queue.empty() and
                    self._trajectory_queue.empty() and
                    len(self._pending_rmq_messages) == 0
            ):
                self._done = True
                continue

            while not self._trajectory_queue.empty():
                source_id = self._trajectory_queue.get()
                self._trajectory(source_id)

            try:
                self._process_one(self.command_queue.get())
            except Exception:
                logger.exception('command processing failed')

            if self._done:
                continue

            if self._final_completion_pending or self._ok_to_complete():
                self._complete_trajectories(self._final_completion_pending)
                self._final_completion_pending = False

    def _ok_to_complete(self):
        # If there have been a lot of fixes since the last completion, we can
        # do one.
        dfix = self._fix_count_total - self._fix_count_last_completion
        if dfix >= self.N_FIXES_FOR_COMPLETION:
            return True

        # If there has been a big change in the time of the last position fix
        # since the last completion, we can do one.
        if (
                self._fix_time_latest is not None and
                self._fix_time_last_completion is not None
        ):
            dtlatest = self._fix_time_latest - self._fix_time_last_completion
            if dtlatest >= self.DT_LATEST_FOR_COMPLETION:
                return True

        # And finally, we don't want to leave it too long in real time between
        # completions.
        dtreal = datetime.now() - self._real_time_last_completion
        if dtreal > self.DT_REAL_FOR_COMPLETIONS:
            return True

        # Otherwise, not yet.
        return False

    def _complete_trajectories(self, final: bool = False):
        self._fix_count_last_completion = self._fix_count_total
        self._fix_time_last_completion = self._fix_time_latest
        self._real_time_last_completion = datetime.now()
        source_ids = self._identify_complete_trajectories(final)
        # logger.info(
        #     'Trajectory completion cycle: %s trajectories',
        #     len(source_ids)
        # )
        for source_id in source_ids:
            self._trajectory_queue.put(source_id)

    def _log_positions(self, fixes_processed):
        old_fix_count = self._fix_count_total
        self._fix_count_total += fixes_processed
        if self._fix_count_total // 1000 != old_fix_count // 1000:
            logger.info(
                'processed %s position fixes',
                round(self._fix_count_total, -3)
            )

    def _process_one(self, command):
        match command:
            case SourcePositionCommand() as cmd:
                self._log_positions(self._source_position(cmd))

            case BatchSourcePositionCommand() as cmd:
                self._log_positions(self._batch_source_position(cmd))

            case SourceErrorCommand(message):
                # We just log the errors here. If a source wants to exit
                # after an error, it will send a StopCommand.
                logger.error('Source error: %s', message)

            case SourceDoneCommand(latest_time):
                self._source_done(latest_time)

            case CompleteCommand():
                self._complete_trajectories()

            case StopCommand():
                # Interrupt: stop immediately!
                self._done = True

            case IngesterStatusCommand(live):
                # TODO: Handle changes in ingester status here.
                if not live:
                    logger.info('Ingester has failed!')

            case RMQCommand() as cmd:
                match cmd.message:
                    case rmq.AckMessage(delivery_tag):
                        # If publication to RabbitMQ was successful, delete all
                        # position fixes from the database for the related source
                        # ID and for all earlier delivery tags.
                        self._process_ack_nack(delivery_tag, delete_trajectory=True)

                    case rmq.NackMessage(delivery_tag):
                        # If publication to RabbitMQ was unsuccessful, don't
                        # delete position fixes from the database for the
                        # related source ID. They will be picked up again in
                        # the next trajectory completion cycle. (But do clear
                        # all earlier delivery tags.)
                        self._process_ack_nack(delivery_tag, delete_trajectory=False)

                    case rmq.RPCMessage() as msg:
                        if msg.endpoint == self.liveness_endpoint:
                            LivenessChecker.send_reply(self.rmq, msg)
                        else:
                            logger.warning(
                                'unknown RPC endpoint: %s', msg.endpoint
                            )

                    case _:
                        logger.warning(
                            'unexpected RMQ message "%s"', cmd.message
                        )

    def _process_ack_nack(self, delivery_tag: int, delete_trajectory: bool):
        if delivery_tag in self._pending_rmq_messages:
            to_delete = [
                tag for tag in self._pending_rmq_messages.keys()
                if tag <= delivery_tag
            ]
            for tag in to_delete:
                if delete_trajectory:
                    self.db.delete_trajectory(
                        self._pending_rmq_messages[tag]
                    )
                del self._pending_rmq_messages[tag]

    def _source_position(self, cmd: SourcePositionCommand) -> int:
        self._fix_time_latest = cmd.time
        self.db.save_position(
            cmd.source_id, cmd.transponder_id, cmd.time,
            cmd.orig, cmd.dest, cmd.callsign, cmd.aircraft_type,
            cmd.lat, cmd.lon, cmd.alt, cmd.alt_gnss,
            cmd.heading, cmd.on_ground
        )
        return 1

    def _batch_source_position(self, cmd: BatchSourcePositionCommand) -> int:
        self._fix_time_latest = cmd.times[-1]
        self.db.save_positions(
            cmd.source_ids, cmd.transponder_ids, cmd.times,
            cmd.origs, cmd.dests, cmd.callsigns, cmd.aircraft_types,
            cmd.lats, cmd.lons, cmd.alts, cmd.alts_gnss,
            cmd.headings, cmd.on_grounds
        )
        return len(cmd.source_ids)

    def _source_done(self, latest_time: datetime):
        # Run a final trajectory completion cycle and mark that we
        # should exit when it's finished.
        self._final_completion_pending = True
        self._done_pending = True

    def _trajectory(self, source_id: str):
        self._trajectory_count += 1
        if self._trajectory_count % 100 == 0:
            logger.info(
                'processed %s trajectories', self._trajectory_count
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

    def _identify_complete_trajectories(self, final: bool = False) -> list[str]:
        if final:
            # Ensure that the trajectory completion horizon is far enough past
            # the time of the last position fix so that all data in the
            # staging database is consumed during the final trajectory
            # completion cycle.
            horizon = self._fix_time_latest + self.completion_delay
        elif self.historical:
            # For historical processing jobs, we use the time of the last
            # position fix as a reference time for calculating the trajectory
            # completion horizon.
            if self._fix_time_latest is None:
                # The case where a completion trajectory is triggered for a
                # historical processing job before we have any position fixes.
                return []
            horizon = self._fix_time_latest - self.completion_delay
        else:
            # For live processing, we use the current time (minus any
            # source-dependent data lag) as the reference.
            horizon = datetime.now() - self.data_lag - self.completion_delay
        return self.db.complete_source_ids(horizon)

    def _complete_trajectory(self, source_id: str) -> int | None:
        # Retrieve all position fixes from database as data frame.
        fixes = self.db.get_trajectory(source_id)
        if len(fixes) == 0:
            return None

        # Build trajectory payload to send to ingester.
        payload = Trajectory.build(self.source, source_id, fixes)

        # Send trajectory payload out over RabbitMQ, returning message number
        # for ACK/NACK processing.
        return self.rmq.send('trajectory', payload)
