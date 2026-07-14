from __future__ import annotations

from abc import ABC, abstractmethod
import logging
import os
from pathlib import Path

from feder_common import DataSource
from feder_server import Trajectory, TrajectoryBatch, write_trajectory_batch_netcdf
import feder_server.rmq as rmq
from feder_server.rmq import RMQ

from .db import DB


logger = logging.getLogger(__name__)


class TrajectorySink(ABC):
    """Delivery boundary for completed receiver trajectory batches."""

    supports_end_of_day = False

    @abstractmethod
    def publish_trajectories(self, source_ids: list[str], batch: TrajectoryBatch) -> None:
        """Publish a non-empty trajectory batch for completed source IDs."""

    def publish_end_of_day(self, batch: TrajectoryBatch) -> None:
        raise RuntimeError("end-of-day markers are not supported by this trajectory sink")

    def pending_count(self) -> int:
        return 0

    def finalize(self) -> None:
        """Flush any sink-owned buffered trajectory state before shutdown."""

    def handle_rmq_message(self, message: rmq.Message) -> None:
        logger.warning('unexpected RMQ message "%s"', message)


class RabbitMQTrajectorySink(TrajectorySink):
    """Trajectory sink that preserves RabbitMQ publish ACK/NACK semantics."""

    supports_end_of_day = True

    def __init__(self, db: DB, rmq_client: RMQ):
        self.db = db
        self.rmq = rmq_client
        self._pending_rmq_messages: dict[int, list[str]] = {}

    def publish_trajectories(self, source_ids: list[str], batch: TrajectoryBatch) -> None:
        message_number = self.rmq.send('trajectory', batch)
        if message_number is not None:
            self._pending_rmq_messages[message_number] = source_ids

    def publish_end_of_day(self, batch: TrajectoryBatch) -> None:
        message_number = self.rmq.send('trajectory', batch)
        if message_number is not None:
            self._pending_rmq_messages[message_number] = []

    def pending_count(self) -> int:
        return len(self._pending_rmq_messages)

    def handle_rmq_message(self, message: rmq.Message) -> None:
        match message:
            case rmq.AckMessage(delivery_tag):
                self._process_ack_nack(delivery_tag, delete_trajectories=True)
            case rmq.NackMessage(delivery_tag):
                self._process_ack_nack(delivery_tag, delete_trajectories=False)
            case _:
                logger.warning('unexpected RMQ message "%s"', message)

    def _process_ack_nack(self, delivery_tag: int, delete_trajectories: bool) -> None:
        if delivery_tag not in self._pending_rmq_messages:
            return
        to_delete = [
            tag for tag in self._pending_rmq_messages.keys()
            if tag <= delivery_tag
        ]
        for tag in to_delete:
            if delete_trajectories:
                for source_id in self._pending_rmq_messages[tag]:
                    self.db.delete_trajectory(source_id)
            del self._pending_rmq_messages[tag]


class NetCDFFileTrajectorySink(TrajectorySink):
    """Trajectory sink that atomically publishes aggregated batches as NetCDF files."""

    def __init__(
            self,
            db: DB,
            output_directory: str | os.PathLike[str],
            receiver_name: str,
            max_trajectories: int = 10_000,
    ):
        if max_trajectories < 1:
            raise ValueError("max_trajectories must be at least 1")
        self.db = db
        self.output_directory = Path(output_directory)
        self.receiver_name = receiver_name
        self.max_trajectories = max_trajectories
        self._sequence = 0
        self._buffered_trajectories: list[Trajectory] = []
        self._buffered_trajectory_count = 0

    def publish_trajectories(self, source_ids: list[str], batch: TrajectoryBatch) -> None:
        self._buffered_trajectories.extend(batch.trajectories)
        self._buffered_trajectory_count = batch.trajectory_count

        for source_id in source_ids:
            self.db.delete_trajectory(source_id)

        if len(self._buffered_trajectories) >= self.max_trajectories:
            self._flush_buffer()

    def finalize(self) -> None:
        self._flush_buffer()

    def _flush_buffer(self) -> None:
        if len(self._buffered_trajectories) == 0:
            return

        batch = TrajectoryBatch(
            trajectories=list(self._buffered_trajectories),
            source=self.receiver_name,
            trajectory_count=self._buffered_trajectory_count,
        )
        self._write_batch(batch)
        self._buffered_trajectories.clear()

    def _write_batch(self, batch: TrajectoryBatch) -> None:
        self._sequence += 1
        final_path = self.output_directory / f"{self.receiver_name}.{self._sequence:08d}.nc"
        tmp_path = self.output_directory / f".{self.receiver_name}.{self._sequence:08d}.nc.tmp.{os.getpid()}"
        try:
            write_trajectory_batch_netcdf(
                tmp_path,
                batch,
                metadata={"receiver_name": self.receiver_name, "sequence": self._sequence},
            )
            self._fsync_file_best_effort(tmp_path)
            os.replace(tmp_path, final_path)
            self._fsync_directory_best_effort(self.output_directory)
        except Exception:
            try:
                tmp_path.unlink(missing_ok=True)
            finally:
                logger.exception("failed to publish NetCDF trajectory batch %s", final_path)
            raise

    @staticmethod
    def _fsync_file_best_effort(path: Path) -> None:
        fd = None
        try:
            fd = os.open(path, os.O_RDONLY)
            os.fsync(fd)
        except OSError:
            logger.debug("best-effort fsync failed for %s", path, exc_info=True)
        finally:
            if fd is not None:
                os.close(fd)

    @staticmethod
    def _fsync_directory_best_effort(path: Path) -> None:
        fd = None
        try:
            fd = os.open(path, os.O_RDONLY)
            os.fsync(fd)
        except OSError:
            logger.debug("best-effort directory fsync failed for %s", path, exc_info=True)
        finally:
            if fd is not None:
                os.close(fd)


def build_trajectory_batch(
        db: DB,
        source: DataSource,
        receiver_name: str,
        source_ids: list[str],
        trajectory_count: int,
) -> tuple[list[str], TrajectoryBatch | None]:
    """Build a batch and return only source IDs that produced payloads."""
    payloads = []
    payload_source_ids = []
    for source_id in source_ids:
        fixes = db.get_trajectory(source_id)
        if len(fixes) == 0:
            continue
        payloads.append(Trajectory.build(source, source_id, fixes))
        payload_source_ids.append(source_id)

    if len(payloads) == 0:
        return [], None

    return payload_source_ids, TrajectoryBatch(
        trajectories=payloads,
        source=receiver_name,
        trajectory_count=trajectory_count,
    )
