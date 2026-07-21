from datetime import datetime, timedelta, timezone
import logging
from queue import PriorityQueue
from threading import Event

from feder_common import DataSource
from feder_server import (
    Config, TrajectoryBatch,
    log_counts, ThreadControl, IngesterLivenessResponse
)
from feder_server.rmq import RMQ
from .commands import (
    Command,
    SourcePositionCommand, BatchSourcePositionCommand, EndOfDayCommand,
    SourceErrorCommand, SourceDoneCommand,
    IngesterStatusCommand, RMQCommand, StopCommand
)
from .db import DB
from .monitoring import (
    fix_counter, last_completion_fix_counter, trajectory_counter,
    latest_fix_time_gauge, last_completion_fix_time_gauge, last_completion_time_gauge,
    ingester_liveness_gauge
)
from .sinks import RabbitMQTrajectorySink, TrajectorySink, build_trajectory_batch


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
            db: DB, command_queue: PriorityQueue,
            rmq: RMQ | None = None,
            source_control: ThreadControl | None = None,
            ingester_liveness_interval: int = 0,
            trajectory_sink: TrajectorySink | None = None,
    ):
        self.config = config
        self.source = source
        self.name = name
        self.historical = historical
        self.db = db
        self.command_queue = command_queue
        if trajectory_sink is None:
            if rmq is None:
                raise ValueError('rmq is required when no trajectory sink is provided')
            trajectory_sink = RabbitMQTrajectorySink(db, rmq)
        self.trajectory_sink = trajectory_sink
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
        self._end_of_day_pending = None
        self._fix_count_total = 0
        self._fix_count_last_completion = 0
        self._fix_time_latest = datetime(1, 1, 1, tzinfo=timezone.utc)
        self._fix_time_last_completion = None
        self._real_time_last_completion = datetime.now(timezone.utc)
        self._trajectory_count = 0
        self._sink_finalized = False
        self.failed = False

        # Flow control: used only for historical processing. For historical
        # jobs, the receiver can process a *lot* of data very quickly (it
        # often has access to data directly in local files and it uses an
        # in-memory scratch database), so it can flood the ingester with data.
        # That means that we need flow control mechanisms. For live jobs, the
        # data flow is more moderate.
        self._ingester_trajectory_counts = []
        self._ingester_ref_times = []

    def run(self) -> None:
        # Process messages from command queue.
        while not self._done and not self._immediate_stop.is_set():
            # If we had got a DONE command and were just waiting for
            # trajectory completion to finish, stop now.
            if self._ready_to_finish():
                self._done = True
                continue

            if len(self._trajectories) != 0 and self._source_is_running():
                self._send_trajectories(
                    self._trajectories[:self.TRAJECTORY_BATCH_SIZE]
                )
                self._trajectories = self._trajectories[self.TRAJECTORY_BATCH_SIZE:]
                if self._ready_to_finish():
                    self._done = True
                    continue
                if self.command_queue.empty():
                    continue

            if (
                    len(self._trajectories) == 0 and
                    self._end_of_day_pending is not None
            ):
                self._send_end_of_day()
                if self._ready_to_finish():
                    self._done = True
                    continue

            try:
                self._process_one(self.command_queue.get())
            except Exception:
                logger.exception('command processing failed')

            if self._done:
                continue

            if (
                    self._fix_count_total > 0 and
                    (self._final_completion_pending or self._ok_to_complete())
            ):
                self._add_trajectories(self._final_completion_pending)
                self._final_completion_pending = False

    def immediate_stop(self) -> None:
        self._immediate_stop.set()
        if self.command_queue.empty():
            self.command_queue.put(StopCommand())

    def _source_is_running(self) -> bool:
        return self.source_control is None or self.source_control.is_running

    def _ready_to_finish(self) -> bool:
        ready = (
            self._done_pending and
            not self._final_completion_pending and
            self.command_queue.empty() and
            len(self._trajectories) == 0 and
            self.trajectory_sink.pending_count() == 0
        )
        if ready and not self._sink_finalized:
            self.trajectory_sink.finalize()
            self._sink_finalized = True
        return ready and self._sink_finalized

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

            case EndOfDayCommand() as cmd:
                self._end_of_day_pending = cmd.day

            case SourceErrorCommand(message, stop):
                # Log errors and stop if requested.
                logger.error('source error: %s', message)
                if stop:
                    self.failed = True
                    self._done = True

            case SourceDoneCommand(latest_time):
                logger.info('SOURCE-DONE')
                self._source_done(latest_time)

            case IngesterStatusCommand() as cmd:
                if self.source_control is None:
                    return
                if self._handle_ingester_status(cmd.response):
                    self.source_control.resume()
                else:
                    self.source_control.pause()

            case RMQCommand() as cmd:
                self.trajectory_sink.handle_rmq_message(cmd.message)

    def _handle_ingester_status(
            self, response: IngesterLivenessResponse | None
    ) -> bool:
        if response is None:
            logger.info('ingester has failed!')
            if not self.historical:
                ingester_liveness_gauge.labels(source=self.name).set(0)
            return False

        if not self.historical:
            ingester_liveness_gauge.labels(source=self.name).set(1)
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

        # Work out how far the ingester is behind and if it's too far, pause
        # the data source.
        delta = self._trajectory_count - self._ingester_trajectory_counts[-1]
        if delta > self.MAX_OUTSTANDING_TRAJECTORIES:
            logger.info(
                'ingester delta too great: %s - %s = %s - waiting...',
                self._trajectory_count, self._ingester_trajectory_counts[-1], delta
            )
            return False

        return True

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

        # Process a set of complete trajectories. If publishing doesn't work,
        # then position fixes remain in the database to be reprocessed in the
        # next completion cycle. File sinks use successful atomic publish as
        # the ACK boundary and delete fixes synchronously after the rename.
        payload_source_ids, batch = build_trajectory_batch(
            self.db, self.source, self.name, source_ids, self._trajectory_count
        )

        # Skip empty payloads: these can confuse the ingester, because we use
        # empty trajectory batches to indicate end-of-day.
        if batch is None:
            return

        self.trajectory_sink.publish_trajectories(payload_source_ids, batch)
        trajectory_counter.labels(source=self.name).inc(delta)

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

    def _send_end_of_day(self) -> None:
        # An empty trajectory batch is sent to the ingester to indicate the
        # end of a day, which allows the ingester to flush the current day's
        # in-memory database to disk.
        assert self._end_of_day_pending is not None
        if self.trajectory_sink.supports_end_of_day:
            self.trajectory_sink.publish_end_of_day(TrajectoryBatch(
                trajectories=[],
                source=self.name,
                trajectory_count=self._end_of_day_pending.toordinal())
            )
        self._end_of_day_pending = None
