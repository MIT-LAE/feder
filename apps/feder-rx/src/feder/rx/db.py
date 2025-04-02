from dataclasses import dataclass
from datetime import datetime
import logging
import os
import sqlite3

import pandas as pd

from feder.server.config import Config


logger = logging.getLogger(__name__)


@dataclass
class Fix:
    transponder_id: str
    time: int
    callsign: str
    aircrafttype: str | None
    lat: float
    lon: float
    alt: float | None
    alt_gnss: float | None
    heading: float | None
    on_ground: bool


class DB:
    def __init__(self, config: Config, source: str, historical: bool = False):
        self.config = config
        self.source = source
        suffix = ''
        self.historical = historical
        if self.historical:
            suffix = f'-{os.getpid()}'
        self.db_path = os.path.join(config.scratch_directory, source + suffix + '.db')
        os.makedirs(config.scratch_directory, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self._ensure_schema()

    def purge(self) -> None:
        logger.info('Purging staging for source "%s"', self.source)
        cur = self.conn.cursor()
        cur.execute("DELETE FROM fixes")
        self.conn.commit()

    def remove(self) -> None:
        if not self.historical:
            raise RuntimeError('attempt to remove live staging database')
        if not self.is_empty():
            raise RuntimeError(
                f'attempt to remove non-empty staging database: {self.db_path}'
            )
        self.conn.close()
        os.remove(self.db_path)

    def is_empty(self) -> bool:
        return self.count_entries() == 0

    def count_entries(self) -> int:
        cur = self.conn.cursor()
        return cur.execute('SELECT COUNT(*) FROM fixes').fetchone()[0]

    def save_position(
            self,
            source_id: str, transponder_id: str, time: datetime,
            callsign: str, aircrafttype: str | None,
            lat: float, lon: float, alt: int | None, alt_gnss: int | None,
            heading: float | None, on_ground: bool
    ) -> None:
        sql = """
          INSERT INTO fixes
            (source_id, transponder_id, time, callsign, aircrafttype,
             lat, lon, alt, alt_gnss, heading, on_ground)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cur = self.conn.cursor()
        cur.execute(
            sql,
            (source_id, transponder_id, int(time.timestamp()),
             callsign, aircrafttype,
            lat, lon, alt, alt_gnss, heading, on_ground)
        )
        self.conn.commit()

    def complete_source_ids(self, horizon: datetime) -> list[str]:
        sql = """
          WITH latest AS (
            SELECT source_id, MAX(time) AS ts FROM fixes GROUP BY source_id
          )
          SELECT source_id FROM latest WHERE ts < ?
        """
        cur = self.conn.cursor()
        return [
            t[0] for t in cur.execute(
                sql, (int(horizon.timestamp()),)
            ).fetchall()
        ]

    def get_trajectory(self, source_id: str) -> pd.DataFrame:
        sql = """
           SELECT transponder_id, time, callsign, aircrafttype,
             lat, lon, alt, alt_gnss, heading, on_ground
             FROM fixes WHERE source_id = ? ORDER BY time
        """
        cur = self.conn.cursor()
        rows = []
        for row in cur.execute(sql, (source_id, )).fetchall():
            rows.append(Fix(
                transponder_id=row[0], time=row[1],
                callsign=row[2], aircrafttype=row[3],
                lat=row[4], lon=row[5], alt=row[6], alt_gnss=row[7],
                heading=row[8], on_ground=row[9]
            ))
        return pd.DataFrame(rows).convert_dtypes()

    def _ensure_schema(self) -> None:
        cur = self.conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS fixes (
        id INTEGER PRIMARY KEY,
        source_id TEXT NOT NULL,
        transponder_id TEXT NOT NULL,
        time INTEGER NOT NULL,
        callsign TEXT NOT NULL,
        aircrafttype TEXT,
        lat FLOAT NOT NULL,
        lon FLOAT NOT NULL,
        alt FLOAT,
        alt_gnss FLOAT,
        heading FLOAT,
        on_ground BOOL DEFAULT FALSE
        )""")

        cur.execute("""CREATE UNIQUE INDEX IF NOT EXISTS fixes_idx ON
                        fixes (source_id, time)""")
