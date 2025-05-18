from datetime import datetime, timedelta, timezone
import logging
from queue import PriorityQueue
from threading import Event

from feder.common import DataSource
from feder.server import (
    Config, RMQ, Trajectory, TrajectoryBatch, Liveness,
    log_counts, ThreadControl, IngesterLivenessResponse
)
import feder.server.rmq as rmq

from .commands import (
    Command,
    SourcePositionCommand, BatchSourcePositionCommand,
    SourceErrorCommand, SourceDoneCommand,
    IngesterStatusCommand, RMQCommand, StopCommand
)
from .db import DB
from .monitoring import (
    fix_counter, last_completion_fix_counter, trajectory_counter,
    latest_fix_time_gauge, last_completion_fix_time_gauge, last_completion_time_gauge
)


logger = logging.getLogger(__name__)


class Processor:
    N_FIXES_FOR_COMPLETION = 1000
    DT_LATEST_FOR_COMPLETION = timedelta(minutes=15)
    DT_REAL_FOR_COMPLETIONS = timedelta(minutes=15)
    TRAJECTORY_BATCH_SIZE = 100
    SLOW_INGESTER_INTERVAL = timedelta(seconds=5)
    INGESTER_AVERAGING_SPAN = 5
    MAX_OUTSTANDING_TRAJECTORIES = 500

    def __init__(
            self,
            config: Config, source: DataSource, name: str, historical: bool,
            db: DB, command_queue: PriorityQueue, rmq: RMQ,
            source_control: ThreadControl,
            ingester_liveness_interval: int
    ):
        self.config = config
        self.source = source
        self.name = name
        self.historical = historical
        self.db = db
        self.command_queue = command_queue
        self.rmq = rmq
        self.data_lag = self.config.data_lag(self.source)
        if self.historical:
            self.data_lag = timedelta(0)
        self.completion_delay = self.config.completion_delay(self.source)
        self.source_control = source_control
        self.ingester_liveness_interval = ingester_liveness_interval

        self._trajectories = []
        self._done = False
        self._immediate_stop = Event()
        self._done_pending = False
        self._final_completion_pending = False
        self._pending_rmq_messages = {}
        self._fix_count_total = 0
        self._fix_count_last_completion = 0
        self._fix_time_latest = datetime(1, 1, 1)
        self._fix_time_last_completion = None
        self._real_time_last_completion = datetime.now(timezone.utc)
        self._trajectory_count = 0

        # Flow control: used only for historical processing. For historical
        # jobs, the receiver can process a *lot* of data very quickly (it
        # often has access to data directly in local files and it uses an
        # in-memory scratch database), so it can flood the ingester with data.
        # That means that we need flow control mechanisms. For live jobs, the
        # data flow is more moderate.
        self._ingester_trajectory_counts = []
        self._ingester_ref_times = []
        self._trajectory_tranche_size = 1000
        self._trajectory_tranche_progress = 0
        self._trajectory_tranche_waiting = False

    def run(self) -> None:
        # Process messages from command queue.
        while not self._done and not self._immediate_stop.is_set():
            # If we had got a DONE command and were just waiting for
            # trajectory completion to finish, stop now.
            if (
                    self._done_pending and
                    not self._final_completion_pending and
                    self.command_queue.empty() and
                    len(self._trajectories) == 0 and
                    len(self._pending_rmq_messages) == 0
            ):
                self._done = True
                continue

            if len(self._trajectories) != 0 and self.source_control.is_running:
                self._send_trajectories(
                    self._trajectories[:self.TRAJECTORY_BATCH_SIZE]
                )
                self._trajectories = self._trajectories[self.TRAJECTORY_BATCH_SIZE:]

            try:
                self._process_one(self.command_queue.get())
            except Exception:
                logger.exception('command processing failed')

            if self._done:
                continue

            if self._final_completion_pending or self._ok_to_complete():
                self._add_trajectories(self._final_completion_pending)
                self._final_completion_pending = False

    def immediate_stop(self) -> None:
        self._immediate_stop.set()
        if self.command_queue.empty():
            self.command_queue.put(StopCommand())

    def _ok_to_complete(self) -> bool:
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
        dtreal = datetime.now(timezone.utc) - self._real_time_last_completion
        if dtreal > self.DT_REAL_FOR_COMPLETIONS:
            return True

        # Otherwise, not yet.
        return False

    def _add_trajectories(self, final: bool = False) -> None:
        delta = self._fix_count_total - self._fix_count_last_completion
        self._fix_count_last_completion = self._fix_count_total
        last_completion_fix_counter.labels(source=self.name).inc(delta)
        self._fix_time_last_completion = self._fix_time_latest
        last_completion_fix_time_gauge.labels(source=self.name).set(self._fix_time_latest.timestamp())
        self._real_time_last_completion = datetime.now(timezone.utc)
        last_completion_time_gauge.labels(source=self.name).set_to_current_time()
        self._trajectories += self._identify_complete_trajectories(final)

    def _log_positions(self, fixes_processed: int) -> None:
        self._fix_count_total = log_counts(
            logger, 'position fixes', self._fix_count_total, fixes_processed, 4
        )

    def _process_one(self, command: Command) -> None:
        match command:
            case StopCommand():
                self._done = True

            case SourcePositionCommand() as cmd:
                self._log_positions(self._source_position(cmd))

            case BatchSourcePositionCommand() as cmd:
                self._log_positions(self._batch_source_position(cmd))

            case SourceErrorCommand(message, stop):
                # Log errors and stop if requested.
                logger.error('source error: %s', message)
                if stop:
                    self._done = True

            case SourceDoneCommand(latest_time):
                logger.info('SOURCE-DONE')
                self._source_done(latest_time)

            case IngesterStatusCommand() as cmd:
                if self._handle_ingester_status(cmd.response):
                    self.source_control.resume()
                else:
                    self.source_control.pause()

            case RMQCommand() as cmd:
                match cmd.message:
                    case rmq.AckMessage(delivery_tag):
                        # If publication to RabbitMQ was successful, delete all
                        # position fixes from the database for the related source
                        # ID and for all earlier delivery tags.
                        self._process_ack_nack(delivery_tag, delete_trajectories=True)

                    case rmq.NackMessage(delivery_tag):
                        # If publication to RabbitMQ was unsuccessful, don't
                        # delete position fixes from the database for the
                        # related source ID. They will be picked up again in
                        # the next trajectory completion cycle. (But do clear
                        # all earlier delivery tags.)
                        self._process_ack_nack(delivery_tag, delete_trajectories=False)

                    case _:
                        logger.warning(
                            'unexpected RMQ message "%s"', cmd.message
                        )

    def _handle_ingester_status(
            self, response: IngesterLivenessResponse
    ) -> bool:
        if response.status != Liveness.OK:
            logger.info('ingester has failed!')
            return False

        if not self.historical:
            return True

        # Flow control for historical jobs follows...

        # If the ingester hasn't ingested anything at all yet, just continue.
        if response.last_ingested == 0:
            return True

        # Collect a rolling sequence of trajectory counts and times for
        # calculating the moving average ingestion rate.
        if len(self._ingester_trajectory_counts) > self.INGESTER_AVERAGING_SPAN:
            self._ingester_trajectory_counts = self._ingester_trajectory_counts[1:]
            self._ingester_ref_times = self._ingester_ref_times[1:]
        self._ingester_trajectory_counts.append(response.last_ingested)
        self._ingester_ref_times.append(datetime.now(timezone.utc))

        # Calculate the moving average ingestion rate.
        if len(self._ingester_trajectory_counts) > 1:
            dtraj = self._ingester_trajectory_counts[-1] - self._ingester_trajectory_counts[0]
            dt = (self._ingester_ref_times[-1] - self._ingester_ref_times[0]).total_seconds()
            ingester_rate = dtraj / dt
        else:
            # Slow start if we don't have enough data to calculate a rate yet.
            ingester_rate = 50

        # Work out how far the ingester is behind and if it's too far, pause
        # the data source.
        delta = self._trajectory_count - self._ingester_trajectory_counts[-1]
        if delta > self.MAX_OUTSTANDING_TRAJECTORIES:
            logger.info(
                'ingester delta too great: %s - %s = %s - waiting...',
                self._trajectory_count, self._ingester_trajectory_counts[-1], delta
            )
            return False

        # If the ingester is not too far behind, we can send trajectories.
        # Calculate a good number to send based on the ingestion rate.
        self._trajectory_tranche_size = round(
            int(5 * ingester_rate * self.ingester_liveness_interval), -2
        )
        self._trajectory_tranche_progress = 0
        self._trajectory_tranche_waiting = False

        return True

    def _process_ack_nack(
            self, delivery_tag: int, delete_trajectories: bool
    ) -> None:
        if delivery_tag in self._pending_rmq_messages:
            to_delete = [
                tag for tag in self._pending_rmq_messages.keys()
                if tag <= delivery_tag
            ]
            for tag in to_delete:
                if delete_trajectories:
                    for source_id in self._pending_rmq_messages[tag]:
                        self.db.delete_trajectory(source_id)
                del self._pending_rmq_messages[tag]

    def _source_position(self, cmd: SourcePositionCommand) -> int:
        self._fix_time_latest = max(self._fix_time_latest, cmd.time)
        self.db.save_position(
            cmd.source_id, cmd.transponder_id, cmd.time,
            cmd.orig, cmd.dest, cmd.callsign, cmd.aircraft_type,
            cmd.lat, cmd.lon, cmd.alt, cmd.alt_gnss,
            cmd.heading, cmd.on_ground
        )
        return 1

    def _batch_source_position(self, cmd: BatchSourcePositionCommand) -> int:
        self._fix_time_latest = max(self._fix_time_latest, *cmd.times)
        self.db.save_positions(
            cmd.source_ids, cmd.transponder_ids, cmd.times,
            cmd.origs, cmd.dests, cmd.callsigns, cmd.aircraft_types,
            cmd.lats, cmd.lons, cmd.alts, cmd.alts_gnss,
            cmd.headings, cmd.on_grounds
        )
        fix_counter.labels(source=self.name).inc(len(cmd.source_ids))
        latest_fix_time_gauge.labels(source=self.name).set(self._fix_time_latest.timestamp())
        return len(cmd.source_ids)

    def _trajectory_tranche_control(self, ntrajs: int) -> None:
        # TODO: DO SOMETHING TO STOP THIS KICKING IN AFTER FILE DOWNLOADS...
        self._trajectory_tranche_progress += ntrajs
        if (
            self._trajectory_tranche_progress >= self._trajectory_tranche_size and
            not self._trajectory_tranche_waiting
        ):
            self._trajectory_tranche_waiting = True
            logger.info(
                'trajectory tranche filled (%s) - waiting...',
                self._trajectory_tranche_progress
            )
            self.source_control.pause()

    def _source_done(self, latest_time: datetime) -> None:
        # Run a final trajectory completion cycle and mark that we
        # should exit when it's finished.
        self._final_completion_pending = True
        self._done_pending = True

    def _send_trajectories(self, source_ids: list[str]) -> None:
        old_count = self._trajectory_count
        self._trajectory_count = log_counts(
            logger, 'trajectories', self._trajectory_count, len(source_ids), 2
        )
        delta = self._trajectory_count - old_count

        # Process a set of complete trajectories. If this doesn't work, then
        # the position fixes for the trajectory will remain in the database to
        # be reprocessed in the next completion cycle.
        #
        # A special case here is if a trajectory completion fails in the final
        # trajectory completion cycle of a historical processing job. (This
        # case just falls through to the "attempt to remove a non-empty
        # staging database" error, since there will be position fixes left in
        # the database when the process exits.)
        #
        # The message number from RabbitMQ is saved for publish confirmation
        # processing.
        payloads = []
        for source_id in source_ids:
            # Retrieve all position fixes from database as data frame.
            fixes = self.db.get_trajectory(source_id)
            if len(fixes) == 0:
                continue

            # Build trajectory payload to send to ingester.
            payloads.append(Trajectory.build(self.source, source_id, fixes))

        # Send trajectory payload out over RabbitMQ, returning message number
        # for ACK/NACK processing.
        message_number = self.rmq.send(
            'trajectory', TrajectoryBatch(
                trajectories=payloads,
                source=self.name,
                trajectory_count=self._trajectory_count)
        )
        trajectory_counter.labels(source=self.name).inc(delta)
        if message_number is not None:
            self._pending_rmq_messages[message_number] = source_ids
        if self.historical:
            self._trajectory_tranche_control(len(payloads))

    def _identify_complete_trajectories(self, final: bool = False) -> list[str]:
        if final:
            # Ensure that the trajectory completion horizon is far enough past
            # the time of the last position fix so that all data in the
            # staging database is consumed during the final trajectory
            # completion cycle.
            horizon = self._fix_time_latest + self.completion_delay
        else:
            # Use the time of the last position fix as a reference time for
            # calculating the trajectory completion horizon.
            if self._fix_time_latest is None:
                # The case where a completion trajectory is triggered for a
                # job before we have any position fixes.
                return []
            horizon = self._fix_time_latest - self.completion_delay
        return self.db.complete_source_ids(horizon)
