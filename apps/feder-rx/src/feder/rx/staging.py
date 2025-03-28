import logging
import os
import sqlite3

from feder.server.config import Config


logger = logging.getLogger(__name__)


class DB:
    def __init__(self, config: Config, source: str):
        self.config = config
        self.source = source
        staging_path = os.path.join(config.scratch_directory, source + '.db')
        os.makedirs(config.scratch_directory, exist_ok=True)
        self.conn = sqlite3.connect(staging_path)
        self._ensure_schema()

    def purge(self) -> None:
        logger.info('Purging staging for source "%s"', self.source)
        cur = self.conn.cursor()
        cur.execute("DELETE FROM fixes")
        self.conn.commit()

    def complete_source_ids(self, horizon: int) -> list[str]:
        sql = """
          WITH latest AS (
            SELECT source_id, MAX(time) AS ts FROM fixes GROUP BY source_id
          )
          SELECT source_id FROM latest WHERE ts < ?
        """
        cur = self.conn.cursor()
        return [t[0] for t in cur.execute(sql, (horizon,)).fetchall()]

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
        alt FLOAT NOT NULL,
        alt_gnss FLOAT,
        heading FLOAT,
        on_ground BOOL DEFAULT FALSE
        )""")

        cur.execute("""CREATE UNIQUE INDEX IF NOT EXISTS fixes_idx ON
                        fixes (source_id, time)""")
