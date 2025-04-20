from datetime import datetime, date
import logging
import os

from feder.common import Trajectory

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
            _, old_conn = self._connections.popitem(last=False)
            old_conn.close()
        return conn

    def close(self) -> None:
        # Close all open database connections.
        for conn in self._connections.values():
            conn.close()
        self._connections.clear()

    def add_trajectory(self, traj: Trajectory) -> int:
        db = self.connect(traj.points[0].time)
        return db.add_trajectory(traj)
