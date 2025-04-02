import csv
from datetime import datetime
import logging

from feder.server.sources import CSV_SOURCE_NAME
from ..commands import SourcePositionCommand
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
        self.aircrafttype_col = row.index('aircrafttype')
        self.lat_col = row.index('lat')
        self.lon_col = row.index('lon')
        self.alt_col = row.index('alt')
        self.alt_gnss_col = row.index('alt_gnss')
        self.heading_col = row.index('heading')
        self.air_ground_col = row.index('air_ground')
        return True


class CSVSource(FileSource):
    NAME = CSV_SOURCE_NAME

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
                self.queue.put(SourcePositionCommand(
                    source_id = row[idx.id_col],
                    transponder_id = row[idx.hexid_col],
                    time = datetime.fromtimestamp(int(row[idx.clock_col])),
                    callsign = row[idx.ident_col],
                    aircrafttype = n(row, idx.aircrafttype_col, str),
                    lat = float(row[idx.lat_col]),
                    lon = float(row[idx.lon_col]),
                    alt = n(row, idx.alt_col, int),
                    alt_gnss = n(row, idx.alt_gnss_col, int),
                    heading = n(row, idx.heading_col, float),
                    on_ground = row[idx.air_ground_col] == 'G'
                ))
