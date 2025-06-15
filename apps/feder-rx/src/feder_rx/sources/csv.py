import csv
import logging
from datetime import datetime, timezone
from queue import Queue
from typing import Generator

from feder_common import DataSource
from feder_server import Config

from ..commands import BatchSourcePositionCommand, Command
from . import FileSource

logger = logging.getLogger(__name__)


# Helper class for handling non-fixed column order in CSV files.

class ColIndex:
    def __init__(self):
        self.complete = False

    def fill(self, row) -> bool:
        if self.complete:
            return False
        self.id_col = row.index('id')
        self.hexid_col = row.index('hexid')
        self.clock_col = row.index('clock')
        self.ident_col = row.index('ident')
        self.orig_col = row.index('orig')
        self.dest_col = row.index('dest')
        self.aircraft_type_col = row.index('aircrafttype')
        self.lat_col = row.index('lat')
        self.lon_col = row.index('lon')
        self.alt_col = row.index('alt')
        self.alt_gnss_col = row.index('alt_gnss')
        self.heading_col = row.index('heading')
        self.air_ground_col = row.index('air_ground')
        self.complete = True
        return True


class CSVSource(FileSource):
    SOURCE = DataSource.FLIGHTAWARE
    NAME = 'csv'
    BATCH_SIZE = 100

    def __init__(self, config: Config, queue: Queue, *args, **kwargs):
        super().__init__(config, queue, *args, **kwargs)
        self._clear()

    def _clear(self):
        self._source_ids = []
        self._transponder_ids = []
        self._times = []
        self._origs = []
        self._dests = []
        self._callsigns = []
        self._aircraft_types = []
        self._lats = []
        self._lons = []
        self._alts = []
        self._alts_gnss = []
        self._headings = []
        self._on_grounds = []
        self._nrows = 0

    def process_file(self, filename) -> Generator[Command, None, None]:
        # Helper for value conversion.
        def n(r, c, xform):
            return None if r[c] == '' else xform(r[c])

        with open(filename, newline='') as fp:
            idx = ColIndex()

            for row in csv.reader(fp):
                # Columns aren't in a fixed order so use a helper to handle
                # the column extraction.
                try:
                    if idx.fill(row):
                        continue
                except Exception:
                    # An exception here means that we have an ill-formed CSV
                    # input, probably because there was an error getting the
                    # original FlightAware data.
                    logger.info('invalid columns in file, skipping %s', filename)
                    return

                # One source position command per row.
                try:
                    self._source_ids.append(row[idx.id_col])
                    self._transponder_ids.append(row[idx.hexid_col])
                    self._times.append(
                        datetime.fromtimestamp(int(row[idx.clock_col]), tz=timezone.utc)
                    )
                    self._callsigns.append(row[idx.ident_col])
                    self._origs.append(n(row, idx.orig_col, str))
                    self._dests.append(n(row, idx.dest_col, str))
                    self._aircraft_types.append(n(row, idx.aircraft_type_col, str))
                    self._lats.append(float(row[idx.lat_col]))
                    self._lons.append(float(row[idx.lon_col]))
                    self._alts.append(n(row, idx.alt_col, int))
                    self._alts_gnss.append(n(row, idx.alt_gnss_col, int))
                    self._headings.append(n(row, idx.heading_col, float))
                    self._on_grounds.append(row[idx.air_ground_col] == 'G')
                except Exception:
                    # Skip bad rows. They're often rows with FlightAware error
                    # messages in them, and we don't care too much about those.
                    logger.exception('error processing row: %s', ','.join(row))
                    continue
                self._nrows += 1

                if self._nrows == self.BATCH_SIZE:
                    yield BatchSourcePositionCommand(
                        self._source_ids, self._transponder_ids, self._times,
                        self._origs, self._dests, self._callsigns,
                        self._aircraft_types,
                        self._lats, self._lons, self._alts, self._alts_gnss,
                        self._headings, self._on_grounds
                    )
                    self._clear()

            if self._nrows > 0:
                yield BatchSourcePositionCommand(
                    self._source_ids, self._transponder_ids, self._times,
                    self._origs, self._dests, self._callsigns,
                    self._aircraft_types,
                    self._lats, self._lons, self._alts, self._alts_gnss,
                    self._headings, self._on_grounds
                )
                self._clear()
