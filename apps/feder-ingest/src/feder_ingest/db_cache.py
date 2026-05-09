from datetime import datetime, date, timedelta
import glob
import logging
import os
import re
import shutil
import sqlite3
import uuid

from feder_common import Trajectory
from feder_server import validate_path_roots

from .writeable_db import WritableDB
from .utils import LastUpdatedOrderedDict


logger = logging.getLogger(__name__)


ConnectionCache = LastUpdatedOrderedDict[date, WritableDB]


class DBCache:
    """LRU cache of writable database connections."""

    def __init__(
            self,
            data_dir: str,
            staging_dir: str,
            scratch_dir: str,
            connection_cache_size: int = 16,
            nursery_size: int = 5,
            export_interval: timedelta = timedelta(hours=1),
            finalize_after: timedelta = timedelta(hours=12),
    ):
        validate_path_roots({
            'data_dir': data_dir,
            'staging_dir': staging_dir,
            'scratch_dir': scratch_dir,
        })
        if not os.path.exists(data_dir):
            raise ValueError(
                f'database directory {data_dir} does not exist'
            )
        self.data_dir = data_dir
        self.staging_dir = staging_dir
        self.scratch_dir = scratch_dir
        self.export_scratch_dir = os.path.join(scratch_dir, 'ingester-export')
        os.makedirs(staging_dir, exist_ok=True)
        os.makedirs(self.export_scratch_dir, exist_ok=True)
        self.connection_cache_size = connection_cache_size
        self.nursery_size = nursery_size
        self.export_interval = export_interval
        self.finalize_after = finalize_after
        self._nursery: ConnectionCache = LastUpdatedOrderedDict()
        self._connections: ConnectionCache = LastUpdatedOrderedDict()
        self._touched: set[date] = set()
        self._dirty: set[date] = set()
        self._last_update: dict[date, datetime] = {}
        self._last_export: dict[date, datetime] = {}
        self._trajectory_count = 0
        self._startup_cleanup()
        self._scan_staging()

    def _startup_cleanup(self) -> None:
        for path in glob.glob(os.path.join(self.staging_dir, '????', '.*.sqlite.importing.*')):
            self._remove_temp_file(path)
        for path in glob.glob(os.path.join(self.export_scratch_dir, '*.export.*.sqlite')):
            self._remove_temp_file(path)
        for path in glob.glob(os.path.join(self.data_dir, '????', '.*.sqlite.exporting.*')):
            self._remove_temp_file(path)

    def _remove_temp_file(self, path: str) -> None:
        try:
            if os.path.isfile(path) or os.path.islink(path):
                os.remove(path)
        except OSError:
            logger.debug('failed to remove temporary file %s', path, exc_info=True)

    def _scan_staging(self) -> None:
        for path in glob.glob(os.path.join(self.staging_dir, '????', '????-???.sqlite')):
            day = self._date_from_db_path(path)
            if day is None:
                continue
            staging_mtime = self._db_mtime(path)
            self._last_update[day] = datetime.fromtimestamp(staging_mtime)
            public_path = WritableDB.db_path(self.data_dir, day)
            if os.path.exists(public_path):
                public_mtime = os.path.getmtime(public_path)
                self._last_export[day] = datetime.fromtimestamp(public_mtime)
                if staging_mtime > public_mtime:
                    self._dirty.add(day)
            else:
                self._dirty.add(day)

    def _date_from_db_path(self, path: str) -> datetime | None:
        match = re.fullmatch(r'(\d{4})-(\d{3})\.sqlite', os.path.basename(path))
        if match is None:
            return None
        try:
            return datetime.strptime(f'{match.group(1)}-{match.group(2)}', '%Y-%j')
        except ValueError:
            return None

    def _db_mtime(self, path: str) -> float:
        mtimes = [os.path.getmtime(path)]
        for suffix in ('-wal', '-shm'):
            sidecar = path + suffix
            if os.path.exists(sidecar):
                mtimes.append(os.path.getmtime(sidecar))
        return max(mtimes)

    def connect(self, ref_date: datetime | date | int) -> WritableDB:
        ref_date = WritableDB.normalize_date(ref_date)

        if ref_date in self._connections and ref_date in self._nursery:
            raise RuntimeError(
                f'database {ref_date.strftime("%Y-%j")} is both staged and in nursery'
            )

        conn = self._connections.get(ref_date)
        if conn is not None:
            return conn
        conn = self._nursery.get(ref_date)
        if conn is not None:
            return conn

        if os.path.exists(WritableDB.db_path(self.staging_dir, ref_date)):
            conn = self._open_staging(ref_date)
            if conn is not None:
                return conn

        if os.path.exists(WritableDB.db_path(self.data_dir, ref_date)):
            conn = self._import_public_to_staging(ref_date)
            if conn is not None:
                return conn

        conn = WritableDB(self.staging_dir, ref_date, in_memory=True)
        self._nursery[ref_date] = conn
        if len(self._nursery) > self.nursery_size:
            self.commit(force=True)
            evict_date = next(iter(self._nursery))
            nursery = self._nursery[evict_date]
            self._promote((evict_date, nursery))
            nursery.close()
            del self._nursery[evict_date]
        return conn

    def close(self) -> None:
        self.commit(force=True)

        for date_db in list(self._nursery.items()):
            day, db = date_db
            if db.size() > 0:
                self._promote(date_db)
            db.close()
            del self._nursery[day]

        for conn in self._connections.values():
            conn.close()
        self._connections.clear()

    def _open_staging(self, ref_date: datetime | date | int) -> WritableDB | None:
        ref_date = WritableDB.normalize_date(ref_date)
        conn = WritableDB(self.staging_dir, ref_date)
        if conn.size() == 0:
            conn.close()
            self._delete_staging_files(ref_date)
            return None
        self._connections[ref_date] = conn
        self._evict_staged_connections()
        return conn

    def _import_public_to_staging(self, ref_date: datetime | date | int) -> WritableDB | None:
        ref_date = WritableDB.normalize_date(ref_date)
        public_path = WritableDB.db_path(self.data_dir, ref_date)
        staging_path = WritableDB.db_path(self.staging_dir, ref_date)
        staging_dir = os.path.dirname(staging_path)
        staging_name = os.path.basename(staging_path)
        hidden_path = os.path.join(
            staging_dir,
            f'.{staging_name}.importing.{os.getpid()}.{uuid.uuid4().hex}'
        )
        os.makedirs(staging_dir, exist_ok=True)

        try:
            with open(public_path, 'rb') as src, open(hidden_path, 'xb') as dst:
                shutil.copyfileobj(src, dst)
                dst.flush()
                os.fsync(dst.fileno())
            os.replace(hidden_path, staging_path)
            self._fsync_dir_best_effort(staging_dir, 'staging database directory')
        except Exception:
            try:
                os.remove(hidden_path)
            except FileNotFoundError:
                pass
            raise

        logger.info('imported public database to staging: %s', ref_date.strftime('%Y-%j'))
        conn = self._open_staging(ref_date)
        if conn is None:
            return None
        return conn

    def _delete_staging_files(self, ref_date: datetime | date | int) -> None:
        path = WritableDB.db_path(self.staging_dir, ref_date)
        for suffix in ('', '-wal', '-shm'):
            try:
                os.remove(path + suffix)
            except FileNotFoundError:
                pass

    def _evict_staged_connections(self) -> None:
        while len(self._connections) > self.connection_cache_size:
            oldest_day, conn = self._connections.popitem(last=False)
            if oldest_day in self._touched:
                self._connections[oldest_day] = conn
                self.commit(force=True)
                if oldest_day in self._touched:
                    raise RuntimeError(
                        f'failed to commit staged database {oldest_day.strftime("%Y-%j")} '
                        'before cache eviction'
                    )
                continue

            conn.close()

    def _promote(self, date_db: tuple[date, WritableDB]) -> None:
        day, nursery = date_db
        day = WritableDB.normalize_date(day)
        if os.path.exists(WritableDB.db_path(self.staging_dir, day)):
            raise RuntimeError(
                f'staging database already exists for {day.strftime("%Y-%j")}'
            )

        logger.info('nursery promotion: %s', day.strftime('%Y-%j'))
        staging_path = WritableDB.db_path(self.staging_dir, day)
        os.makedirs(os.path.dirname(staging_path), exist_ok=True)
        nursery.commit()
        nursery.cursor().execute('VACUUM INTO ?', (staging_path,))

        staged = WritableDB(self.staging_dir, day)
        self._connections[day] = staged
        self._evict_staged_connections()
        self._dirty.add(day)
        try:
            self._export_db(staged, day)
        except Exception:
            logger.exception('failed to export promoted database: %s', day.strftime('%Y-%j'))

    def checkpoint(self) -> None:
        self.commit(force=True)
        now = datetime.now()
        for day, db in list(self._nursery.items()):
            if db.size() > 0 and self._should_export(day, now):
                logger.info('nursery checkpoint: %s', day.strftime('%Y-%j'))
                try:
                    self._export_db(db, day)
                except Exception:
                    logger.exception('failed to export nursery database: %s', day.strftime('%Y-%j'))
        for day in list(self._dirty):
            if self._should_export(day, now):
                try:
                    self._export_staged(day)
                except Exception:
                    logger.exception('failed to export staged database: %s', day.strftime('%Y-%j'))
        self._finalize_idle(now)

    def _export_staged(self, day: datetime | date | int) -> None:
        day = WritableDB.normalize_date(day)
        db = self._connections.get(day)
        if db is not None:
            self._export_db(db, day)
        else:
            self._export_staged_file(day)

    def _export_staged_file(self, day: datetime | date | int) -> None:
        day = WritableDB.normalize_date(day)
        staging_path = WritableDB.db_path(self.staging_dir, day)
        snapshot_path = os.path.join(
            self.export_scratch_dir,
            f'{day.strftime("%Y-%j")}.export.{os.getpid()}.{uuid.uuid4().hex}.sqlite'
        )
        conn = self._open_raw_staging_connection(staging_path)
        try:
            conn.execute('VACUUM INTO ?', (snapshot_path,))
            snapshot_conn = sqlite3.connect(snapshot_path)
            try:
                snapshot_conn.execute('PRAGMA journal_mode=DELETE')
            finally:
                snapshot_conn.close()
            self._publish_snapshot(snapshot_path, day)
            self._last_export[day] = datetime.now()
            self._dirty.discard(day)
        finally:
            conn.close()
            try:
                os.remove(snapshot_path)
            except FileNotFoundError:
                pass

    def _open_raw_staging_connection(self, staging_path: str) -> sqlite3.Connection:
        uri = f'file:{staging_path}?mode=ro'
        try:
            return sqlite3.connect(uri, uri=True)
        except sqlite3.Error as exc:
            raise RuntimeError(
                f'failed to open staged database in read-only mode for export: {staging_path}'
            ) from exc

    def _finalize_idle(self, now: datetime) -> None:
        for day, last_update in list(self._last_update.items()):
            if day in self._nursery:
                continue
            if now - last_update >= self.finalize_after:
                try:
                    self._finalize_staging(day)
                except Exception:
                    logger.exception('failed to finalize staged database: %s', day.strftime('%Y-%j'))

    def _finalize_staging(self, day: datetime | date | int) -> None:
        day = WritableDB.normalize_date(day)
        if not os.path.exists(WritableDB.db_path(self.staging_dir, day)):
            self._remove_metadata(day)
            return

        self.commit(force=True)
        self._export_staged(day)

        conn = self._connections.pop(day, None)
        if conn is not None:
            conn.close()
        self._delete_staging_files(day)
        self._remove_metadata(day)
        logger.info('finalized staged database: %s', day.strftime('%Y-%j'))

    def _remove_metadata(self, day: datetime | date | int) -> None:
        day = WritableDB.normalize_date(day)
        self._dirty.discard(day)
        self._touched.discard(day)
        self._last_update.pop(day, None)
        self._last_export.pop(day, None)

    def _should_export(self, day: date, now: datetime) -> bool:
        last_export = self._last_export.get(day)
        return last_export is None or now - last_export >= self.export_interval

    def _export_db(self, db: WritableDB, ref_date: datetime | date | int) -> None:
        """Export a clean SQLite snapshot and publish it atomically."""
        ref_date = WritableDB.normalize_date(ref_date)
        db.commit()

        snapshot_path = os.path.join(
            self.export_scratch_dir,
            f'{ref_date.strftime("%Y-%j")}.export.{os.getpid()}.{uuid.uuid4().hex}.sqlite'
        )
        try:
            cur = db.cursor()
            cur.execute('VACUUM INTO ?', (snapshot_path,))

            snapshot_conn = sqlite3.connect(snapshot_path)
            try:
                snapshot_conn.execute('PRAGMA journal_mode=DELETE')
            finally:
                snapshot_conn.close()

            self._publish_snapshot(snapshot_path, ref_date)
            self._last_export[ref_date] = datetime.now()
            self._dirty.discard(ref_date)
        finally:
            try:
                os.remove(snapshot_path)
            except FileNotFoundError:
                pass

    def _publish_snapshot(self, snapshot_path: str, ref_date: datetime | date | int) -> None:
        """Copy a snapshot to a hidden public temp, then replace atomically."""
        final_path = WritableDB.db_path(self.data_dir, ref_date)
        final_dir = os.path.dirname(final_path)
        final_name = os.path.basename(final_path)
        hidden_path = os.path.join(
            final_dir,
            f'.{final_name}.exporting.{os.getpid()}.{uuid.uuid4().hex}'
        )
        os.makedirs(final_dir, exist_ok=True)

        try:
            with open(snapshot_path, 'rb') as src, open(hidden_path, 'xb') as dst:
                shutil.copyfileobj(src, dst)
                dst.flush()
                os.fsync(dst.fileno())
            os.replace(hidden_path, final_path)
            self._fsync_dir_best_effort(final_dir, 'public database directory')
        except Exception:
            try:
                os.remove(hidden_path)
            except FileNotFoundError:
                pass
            raise

    def _fsync_dir_best_effort(self, path: str, description: str) -> None:
        try:
            dir_fd = os.open(path, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            logger.debug('failed to fsync %s', description, exc_info=True)

    def commit(self, force: bool = False) -> None:
        do_commit = False
        if force:
            do_commit = len(self._touched) > 0
        else:
            do_commit = len(self._touched) > 5 or self._trajectory_count % 1000 == 0
        if do_commit:
            logger.info(
                'committing %d trajectories, %d databases',
                self._trajectory_count, len(self._touched)
            )
            for day in self._touched:
                db = self._connections.get(day) or self._nursery.get(day)
                if db is not None:
                    db.commit()
            self._touched.clear()
            self._trajectory_count = 0

    def end_of_day(self, day: date) -> None:
        day = WritableDB.normalize_date(day)
        logger.info(
            'end of day: committing and promoting nursery entries for %s',
            day.strftime('%Y-%j')
        )
        self.commit(force=True)
        if day in self._nursery:
            db = self._nursery[day]
            if db.size() > 0:
                self._promote((day, db))
            db.close()
            del self._nursery[day]
        elif day in self._connections or os.path.exists(WritableDB.db_path(self.staging_dir, day)):
            try:
                self._export_staged(day)
            except Exception:
                logger.exception('failed to export end-of-day database: %s', day.strftime('%Y-%j'))

    def _mark_touched(self, db: WritableDB) -> None:
        day = db.ref_date
        self._touched.add(day)
        self._dirty.add(day)
        self._last_update[day] = datetime.now()

    def add_trajectory(self, traj: Trajectory):
        # Add a trajectory to the appropriate database. "Appropriate" means
        # the database file for the date on which the timestamp of the first
        # point in the trajectory falls.
        #
        # There is a difficulty here, which is that the ingester has to deal
        # with the case where a receiver sends partial trajectories, i.e.
        # trajectory records that do not contain all the points of the full
        # trajectory. If the ingester receives a partial trajectory for a
        # given source ID, then later receives another partial trajectory for
        # the same source ID, it needs to merge the trajectories and save them
        # to the correct database file, removing any old partial trajectories
        # received earlier.
        #
        # If the partial trajectories were guaranteed to belong in the same
        # database file, there wouldn't be a problem. However, trajectories
        # can span multiple days. (For simplicity and realism, we assume that
        # a trajectory cannot span more than three adjacent days.)
        #
        # The end result is that there is some very dubious stuff going on
        # here. We need to manipulate up to three database files at once (that
        # case is very unlikely; the case of needing to manipulate two
        # database files at once is more likely; and the case of only needing
        # to manipulate a single file is most likely, which is something we
        # can special case to avoid the dubiousness...).
        #
        # The problem is that we *cannot* have a single transaction spanning
        # multiple database files! That means that there are potentially some
        # race conditions here. They're rare though, and there's not a simple
        # way to get around them, so let's pretend it's not a problem and
        # forge bravely on. Things are simplified somewhat by allowing only
        # the ingester to modify the database files, but it's still a little
        # awkward.

        db_0 = self.connect(traj.points[0].time)
        db_m1 = self.connect(traj.points[0].time - timedelta(days=1))
        db_p1 = self.connect(traj.points[0].time + timedelta(days=1))

        traj_0 = db_0.get_flight_by_source_id(traj.source, traj.source_id)
        traj_m1 = db_m1.get_flight_by_source_id(traj.source, traj.source_id)
        traj_p1 = db_p1.get_flight_by_source_id(traj.source, traj.source_id)

        if traj_0 is not None or traj_m1 is not None or traj_p1 is not None:
            traj = traj.merge(traj_m1, traj_0, traj_p1)

        if traj_m1 is not None:
            db_m1.delete_trajectory(traj_m1, commit=False)
            self._mark_touched(db_m1)
        if traj_p1 is not None:
            db_p1.delete_trajectory(traj_p1, commit=False)
            self._mark_touched(db_p1)

        if traj_0 is not None:
            db_0.delete_trajectory(traj_0, commit=False)
        db_0.add_trajectory(traj, commit=False)
        self._mark_touched(db_0)
        self._trajectory_count += 1

        self.commit()
