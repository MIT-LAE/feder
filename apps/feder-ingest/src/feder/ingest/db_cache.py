from datetime import datetime
import logging
import os

from feder.server.trajectory_pb2 import Trajectory
from .writeable_db import WritableDB
from .utils import LastUpdatedOrderedDict


logger = logging.getLogger(__name__)


class DBCache:
    """"LRU cache of writable database connections."""

    def __init__(self, data_dir: str, connection_cache_size: int = 16):
        if not os.path.exists(data_dir):
            raise ValueError(
                f'database directory {data_dir} does not exist'
            )
        self.data_dir = data_dir
        self.connection_cache_size = connection_cache_size
        self._connections: LastUpdatedOrderedDict[str, WritableDB] = LastUpdatedOrderedDict()

    def _db_file(self, d: datetime | int) -> str:
        # One database file per day named after year and day of year.
        if isinstance(d, int):
            d = datetime.fromtimestamp(d)
        yr = d.year
        doy = d.timetuple().tm_yday
        return os.path.join(self.data_dir, f'{yr:04d}-{doy:03d}.sqlite')

    def connect(self, d: datetime | int) -> WritableDB:
        if isinstance(d, int):
            d = datetime.fromtimestamp(d)
        logger.info('connect to DB for %s', d.strftime('%Y-%j'))

        # Open database connection for the given date, retrieving it from the
        # cache if it exists. The size of the cache is kept at the size
        # specified in the constructor.
        db_file = self._db_file(d)
        conn = self._connections.get(db_file, WritableDB(db_file))
        self._connections[db_file] = conn
        if len(self._connections) > self.connection_cache_size:
            _, old_conn = self._connections.popitem(last=False)
            old_conn.close()
        return conn

    def close(self) -> None:
        # Close all open database connections.
        for conn in self._connections.values():
            conn.close()
        self._connections.clear()

    def add_trajectory(self, traj: Trajectory) -> int:
        db = self.connect(traj.points.time[0])
        return db.add_trajectory(traj)
