from datetime import datetime, date, timedelta
import logging
import os

from feder_common import Trajectory

from .writeable_db import WritableDB
from .utils import LastUpdatedOrderedDict


logger = logging.getLogger(__name__)


ConnectionCache = LastUpdatedOrderedDict[date, WritableDB]


class DBCache:
    """"LRU cache of writable database connections."""

    def __init__(
            self,
            data_dir: str,
            connection_cache_size: int = 16,
            nursery_size: int = 5
    ):
        if not os.path.exists(data_dir):
            raise ValueError(
                f'database directory {data_dir} does not exist'
            )
        self.data_dir = data_dir
        self.connection_cache_size = connection_cache_size
        self.nursery_size = nursery_size
        self._nursery: ConnectionCache = LastUpdatedOrderedDict()
        self._connections: ConnectionCache = LastUpdatedOrderedDict()
        self._touched = set[WritableDB]()
        self._trajectory_count = 0

    def connect(self, ref_date: datetime | date | int) -> WritableDB:
        ref_date = WritableDB.normalize_date(ref_date)

        # Open database connection for the given date, retrieving it from the
        # cache if it exists. The size of the cache is kept at the size
        # specified in the constructor. The logic here is a little
        # complicated, because we maintain a "nursery" of new databases in
        # memory. These in-memory databases are eventually "promoted" to files
        # in the main data directory.

        # The date is in the main cache (so we use the cached connection) or
        # the nursery (so we use the in-memory nursery connection).
        conn = self._connections.get(ref_date) or self._nursery.get(ref_date)
        if conn is not None:
            return conn

        # A file exists in the main data directory: we open the file in the
        # data directory, put the connection in the connection cache, and
        # evict an entry from the cache if necessary.
        if os.path.exists(WritableDB.db_path(self.data_dir, ref_date)):
            conn = WritableDB(self.data_dir, ref_date)

            # If the database is empty, delete it and fall through to create a
            # new in-memory database.
            if conn.size() == 0:
                conn = None
                os.remove(WritableDB.db_path(self.data_dir, ref_date))
            else:
                self._connections[ref_date] = conn
                if len(self._connections) > self.connection_cache_size:
                    self._connections.popitem(last=False)
                return conn

        # Otherwise this is a new date that we don't have a database for yet.
        # Create a new in-memory database connection in the nursery, cache it
        # and handle any eviction of a connection from the nursery by
        # promoting it to an file on disk.
        conn = WritableDB(self.data_dir, ref_date, in_memory=True)
        self._nursery[ref_date] = conn
        if len(self._nursery) > self.nursery_size:
            self.commit(force=True)
            self._promote(self._nursery.popitem(last=False))
        return conn

    def close(self) -> None:
        self.commit(force=True)

        # Promote cached nursery connections to files.
        for date_db in self._nursery.items():
            self._promote(date_db)
        self._nursery.clear()

        # Close all open database connections.
        for conn in self._connections.values():
            conn.close()
        self._connections.clear()

    def _promote(self, date_db: tuple[date, WritableDB]) -> None:
        date, conn = date_db
        logger.info('nursery promotion: %s', date.strftime('%Y-%j'))
        cur = conn.cursor()
        path = WritableDB.db_path(self.data_dir, date)
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
        os.makedirs(os.path.dirname(path), exist_ok=True)
        cur.execute(f"VACUUM INTO '{WritableDB.db_path(self.data_dir, date)}'")

    def checkpoint(self) -> None:
        self.commit(force=True)
        for day in self._nursery:
            db = self._nursery[day]
            if db.size() > 0:
                logger.info('nursery checkpoint: %s', day.strftime('%Y-%j'))
                self._promote((day, db))

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
            for db in self._touched:
                db.commit()
            self._touched.clear()
            self._trajectory_count = 0

    def end_of_day(self, day: date) -> None:
        day = WritableDB.normalize_date(day)
        logger.info(
            'end of day: committing and promoting nursery entries for %s',
            day.strftime('%Y-%j')
        )
        if day in self._nursery:
            db = self._nursery[day]
            del self._nursery[day]
            self.commit(force=True)
            self._promote((day, db))

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

        # Database connections for the day of the first point in the
        # trajectory and the day before and day after.
        # tic0 = time.perf_counter()
        db_0 = self.connect(traj.points[0].time)
        db_m1 = self.connect(traj.points[0].time - timedelta(days=1))
        db_p1 = self.connect(traj.points[0].time + timedelta(days=1))

        # Any existing trajectories in any of those databases?
        traj_0 = db_0.get_flight_by_source_id(traj.source, traj.source_id)
        traj_m1 = db_m1.get_flight_by_source_id(traj.source, traj.source_id)
        traj_p1 = db_p1.get_flight_by_source_id(traj.source, traj.source_id)

        # The trajectory is partial, since we've found parts of it already in
        # one of these database files, so merge the trajectory data.
        if traj_0 is not None or traj_m1 is not None or traj_p1 is not None:
            traj = traj.merge(traj_m1, traj_0, traj_p1)

        # The trajectory to be merged spans multiple database files. Nothing
        # we can do but handle that like the barbarians we are.
        if traj_m1 is not None:
            db_m1.delete_trajectory(traj_m1, commit=False)
            self._touched.add(db_m1)
        if traj_p1 is not None:
            db_p1.delete_trajectory(traj_p1, commit=False)
            self._touched.add(db_p1)

        # The rest of what we need to do operates on a single database file,
        # we *can* do it in a single transaction.
        if traj_0 is not None:
            db_0.delete_trajectory(traj_0, commit=False)
        db_0.add_trajectory(traj, commit=False)
        self._touched.add(db_0)
        self._trajectory_count += 1

        # Maybe commit, if we've processed enough trajectories.
        self.commit()
