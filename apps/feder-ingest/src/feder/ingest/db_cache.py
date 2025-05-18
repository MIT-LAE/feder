from datetime import datetime, date, timedelta
import os

from feder.common import Trajectory

from .writeable_db import WritableDB
from .utils import LastUpdatedOrderedDict


class DBCache:
    """"LRU cache of writable database connections."""

    def __init__(self, data_dir: str, connection_cache_size: int = 16):
        if not os.path.exists(data_dir):
            raise ValueError(
                f'database directory {data_dir} does not exist'
            )
        self.data_dir = data_dir
        self.connection_cache_size = connection_cache_size
        self._connections: LastUpdatedOrderedDict[date, WritableDB] = LastUpdatedOrderedDict()

    def connect(self, ref_date: datetime | date | int) -> WritableDB:
        if isinstance(ref_date, int):
            ref_date = datetime.fromtimestamp(ref_date)
        if isinstance(ref_date, datetime):
            ref_date = ref_date.date()

        # Open database connection for the given date, retrieving it from the
        # cache if it exists. The size of the cache is kept at the size
        # specified in the constructor.
        conn = self._connections.get(ref_date, WritableDB(self.data_dir, ref_date))
        self._connections[ref_date] = conn
        if len(self._connections) > self.connection_cache_size:
            self._connections.popitem(last=False)
        return conn

    def close(self) -> None:
        # Close all open database connections.
        for conn in self._connections.values():
            conn.close()
        self._connections.clear()

    def add_trajectory(self, traj: Trajectory) -> set[WritableDB]:
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

        touched = set()

        # Database connections for the day of the first point in the
        # trajectory and the day before and day after.
        # tic0 = time.perf_counter()
        db_0 = self.connect(traj.points[0].time)
        db_m1 = self.connect(traj.points[0].time - timedelta(days=1))
        db_p1 = self.connect(traj.points[0].time + timedelta(days=1))

        # Any existing trajectories in any of those databases?
        traj_0 = db_0.get_flight_by_id(traj.source, traj.source_id)
        traj_m1 = db_m1.get_flight_by_id(traj.source, traj.source_id)
        traj_p1 = db_p1.get_flight_by_id(traj.source, traj.source_id)

        # The trajectory is partial, since we've found parts of it already in
        # one of these database files, so merge the trajectory data.
        if traj_0 is not None or traj_m1 is not None or traj_p1 is not None:
            traj = traj.merge(traj_m1, traj_0, traj_p1)

        # The trajectory to be merged spans multiple database files. Nothing
        # we can do but handle that like the barbarians we are.
        if traj_m1 is not None:
            db_m1.delete_trajectory(traj_m1, commit=False)
            touched.add(db_m1)
        if traj_p1 is not None:
            db_p1.delete_trajectory(traj_p1, commit=False)
            touched.add(db_p1)

        # The rest of what we need to do operates on a single database file,
        # we *can* do it in a single transaction.
        if traj_0 is not None:
            db_0.delete_trajectory(traj_0, commit=False)
        db_0.add_trajectory(traj, commit=False)
        touched.add(db_0)

        return touched
