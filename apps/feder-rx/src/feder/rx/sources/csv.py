import glob
import itertools
import logging
import os
import sys
from typing import TYPE_CHECKING

from feder.server.sources import CSV_SOURCE_NAME


if TYPE_CHECKING:
    from feder.server import Config


logger = logging.getLogger(__name__)


# TODO: This should be derived from some base class that handles all the
# database stuff, as well as any queuing/asynchrony.

class CSVSource:
    NAME = CSV_SOURCE_NAME

    def __init__(self, config: 'Config'):
        self.config = config
        self.staging_path = os.path.join(
            self.config.scratch_directory, self.NAME + '.db'
        )

    def purge_staging(self):
        logger.info('Purging staging for source "%s"', self.NAME)

    def run(self, *csv_files: str):
        expanded_csv_files = list(itertools.chain.from_iterable(
            (glob.glob(f) if '*' in f else [f])
            for f in list(csv_files)
        ))

        if len(expanded_csv_files) == 0:
            logger.error('No files provided to CSV source!')
            sys.exit(1)

        self.csv_files = expanded_csv_files
        for f in self.csv_files:
            logger.info('Processing %s', f)
