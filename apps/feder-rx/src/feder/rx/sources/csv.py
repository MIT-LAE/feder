import csv
from datetime import datetime
import logging
from queue import PriorityQueue

from feder.server import Config

from feder.common import DataSource
from ..commands import BatchSourcePositionCommand
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

    def __init__(self, config: Config, queue: PriorityQueue, *args: str):
        super().__init__(config, queue, *args)
        self._clear()

    def _clear(self):
        self._source_ids = []
        self._transponder_ids = []
        self._times = []
        self._callsigns = []
        self._aircraft_types = []
        self._lats = []
        self._lons = []
        self._alts = []
        self._alts_gnss = []
        self._headings = []
        self._on_grounds = []
        self._nrows = 0

    def process_file(self, filename):
        logger.info('Processing CSV: %s', filename)

        # Helper for value conversion.
        def n(r, c, xform):
            return None if r[c] == '' else xform(r[c])

        with open(filename, newline='') as fp:
            idx = ColIndex()

            for row in csv.reader(fp):
                # Columns aren't in a fixed order so use a helper to handle
                # the column extraction.
                if idx.fill(row):
                    continue

                # One source position command per row.
                self._source_ids.append(row[idx.id_col])
                self._transponder_ids.append(row[idx.hexid_col])
                self._times.append(datetime.fromtimestamp(int(row[idx.clock_col])))
                self._callsigns.append(row[idx.ident_col])
                self._aircraft_types.append(n(row, idx.aircraft_type_col, str))
                self._lats.append(float(row[idx.lat_col]))
                self._lons.append(float(row[idx.lon_col]))
                self._alts.append(n(row, idx.alt_col, int))
                self._alts_gnss.append(n(row, idx.alt_gnss_col, int))
                self._headings.append(n(row, idx.heading_col, float))
                self._on_grounds.append(row[idx.air_ground_col] == 'G')
                self._nrows += 1

                if self._nrows == self.BATCH_SIZE:
                    yield BatchSourcePositionCommand(
                        self._source_ids, self._transponder_ids, self._times,
                        self._callsigns, self._aircraft_types,
                        self._lats, self._lons, self._alts, self._alts_gnss,
                        self._headings, self._on_grounds
                    )
                    self._clear()

            if self._nrows > 0:
                yield BatchSourcePositionCommand(
                    self._source_ids, self._transponder_ids, self._times,
                    self._callsigns, self._aircraft_types,
                    self._lats, self._lons, self._alts, self._alts_gnss,
                    self._headings, self._on_grounds
                )
                self._clear()
