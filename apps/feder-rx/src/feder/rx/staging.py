import logging
import os
import sqlite3

from feder.server.config import Config


logger = logging.getLogger(__name__)


def open(cfg: Config, source: str) -> sqlite3.Connection:
    staging_path = os.path.join(cfg.scratch_directory, source + '.db')
    os.makedirs(cfg.scratch_directory, exist_ok=True)
    conn = sqlite3.connect(staging_path)
    _ensure_schema(conn)
    return conn


def purge(cfg: Config, source: str) -> None:
    logger.info('Purging staging for source "%s"', source)
    conn = open(cfg, source)
    cur = conn.cursor()
    cur.execute("DELETE FROM fixes")
    conn.commit()


def _ensure_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS fixes (
      id INTEGER PRIMARY KEY,
      source TEXT NOT NULL,
      source_id TEXT NOT NULL,
      transponder_id TEXT NOT NULL,
      time INTEGER NOT NULL,
      callsign TEXT NOT NULL,
      aircrafttype TEXT,
      lat FLOAT NOT NULL,
      lon FLOAT NOT NULL,
      alt FLOAT NOT NULL,
      heading FLOAT,
      on_ground BOOL DEFAULT FALSE
    )""")
