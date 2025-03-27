import glob
import itertools
import logging
import os
from queue import Queue
import sys
from typing import TYPE_CHECKING

from feder.server.sources import CSV_SOURCE_NAME
from . import Source


if TYPE_CHECKING:
    from feder.server import Config


logger = logging.getLogger(__name__)


# TODO: This should be derived from some base class that handles all the
# database stuff, as well as any queuing/asynchrony.

class CSVSource(Source):
    NAME = CSV_SOURCE_NAME

    def __init__(self, config: 'Config', queue: Queue, *files: str):
        super().__init__(config, queue, files)
        self.staging_path = os.path.join(
            self.config.scratch_directory, self.NAME + '.db'
        )

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
