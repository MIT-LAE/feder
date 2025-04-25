from datetime import datetime
import logging
import os
import sqlite3

from feder.server import Config, Fix


logger = logging.getLogger(__name__)


class DB:
    def __init__(self, config: Config, name: str, historical: bool = False):
        self.config = config
        self.name = name
        self.historical = historical
        if historical:
            self.db_path = ':memory:'
        else:
            self.db_path = os.path.join(config.scratch_directory, name + '.db')
            os.makedirs(config.scratch_directory, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self._ensure_schema()

    def purge(self) -> None:
        logger.info('Purging staging for source "%s"', self.name)
        cur = self.conn.cursor()
        cur.execute("DELETE FROM fixes")
        self.conn.commit()

    def remove(self, force: bool) -> None:
        if not self.historical:
            raise RuntimeError('attempt to remove live staging database')
        if not self.is_empty() and not force:
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
            orig: str | None, dest: str | None, callsign: str,
            aircraft_type: str | None,
            lat: float, lon: float, alt: int | None, alt_gnss: int | None,
            heading: float | None, on_ground: bool
    ) -> None:
        sql = """
          INSERT INTO fixes
            (source_id, transponder_id, time, orig, dest, callsign,
             aircraft_type, lat, lon, alt, alt_gnss, heading, on_ground)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cur = self.conn.cursor()
        cur.execute(
            sql,
            (source_id, transponder_id, int(time.timestamp()),
             orig, dest, callsign, aircraft_type,
            lat, lon, alt, alt_gnss, heading, on_ground)
        )
        self.conn.commit()

    def save_positions(
            self,
            source_ids: list[str], transponder_ids: list[str],
            times: list[datetime],
            origs: list[str | None], dests: list[str | None],
            callsigns: list[str], aircraft_types: list[str | None],
            lats: list[float], lons: list[float],
            alts: list[int | None], alts_gnss: list[int | None],
            headings: list[float | None], on_grounds: list[bool]
    ) -> None:
        sql = """
          INSERT INTO fixes
            (source_id, transponder_id, time, orig, dest, callsign,
             aircraft_type, lat, lon, alt, alt_gnss, heading, on_ground)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        values = [
            (source_ids[i], transponder_ids[i], int(times[i].timestamp()),
             origs[i], dests[i], callsigns[i], aircraft_types[i],
             lats[i], lons[i], alts[i], alts_gnss[i],
             headings[i], on_grounds[i]) for i in range(len(source_ids))]
        cur = self.conn.cursor()
        try:
            cur.executemany(sql, values)
        except Exception:
            logger.exception('insert into DB failed for %s positions', len(source_ids))
            self.conn.rollback()
            return
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

    def get_trajectory(self, source_id: str) -> list[Fix]:
        sql = """
           SELECT transponder_id, time, orig, dest, callsign,
             aircraft_type, lat, lon, alt, alt_gnss, heading, on_ground
             FROM fixes WHERE source_id = ? ORDER BY time
        """
        cur = self.conn.cursor()
        return [
            Fix(
                transponder_id=row[0], time=row[1],
                orig=row[2], dest=row[3],
                callsign=row[4], aircraft_type=row[5],
                lat=row[6], lon=row[7], alt=row[6], alt_gnss=row[8],
                heading=row[10], on_ground=row[11]
            )
            for row in cur.execute(sql, (source_id,)).fetchall()
        ]

    def delete_trajectory(self, source_id: str) -> None:
        sql = 'DELETE FROM fixes WHERE source_id = ?'
        cur = self.conn.cursor()
        cur.execute(sql, (source_id, ))
        self.conn.commit()

    def _ensure_schema(self) -> None:
        cur = self.conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS fixes (
        id INTEGER PRIMARY KEY,
        source_id TEXT NOT NULL,
        transponder_id TEXT NOT NULL,
        time INTEGER NOT NULL,
        orig TEXT,
        dest TEXT,
        callsign TEXT NOT NULL,
        aircraft_type TEXT,
        lat FLOAT NOT NULL,
        lon FLOAT NOT NULL,
        alt FLOAT,
        alt_gnss FLOAT,
        heading FLOAT,
        on_ground BOOL DEFAULT FALSE
        )""")

        cur.execute("""CREATE INDEX IF NOT EXISTS fixes_idx ON
                        fixes (source_id, time)""")
