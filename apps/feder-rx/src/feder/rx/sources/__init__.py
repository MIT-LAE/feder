from abc import abstractmethod
from datetime import datetime, timedelta
import glob
import itertools
import logging
from queue import PriorityQueue
import sys
from threading import Thread, Event
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from feder.server import Config

from ..utils import round_time
from ..commands import SourceDoneCommand


logger = logging.getLogger(__name__)


class Source(Thread):
    NAME = None

    def __init__(self, config: 'Config', queue: PriorityQueue, *args: str):
        super().__init__()
        self.config = config
        self.queue = queue
        self.args = args
        if self.NAME is None:
            raise ValueError('unknown source name')
        self.wait_finished = Event()
        self.stopped = False

    def stop(self):
        self.stopped = True
        self.wait_finished.set()

    def wait_for(self, t: datetime) -> bool:
        delta = t - datetime.now()
        if delta < timedelta(0):
            return self.stopped
        self.wait_finished.wait(delta.total_seconds())
        self.wait_finished = Event()
        return self.stopped

    @abstractmethod
    def run(self):
        ...


class FileSource(Source):
    @abstractmethod
    def process_file(self, filename: str):
        ...

    def run(self):
        expanded_csv_files = list(itertools.chain.from_iterable(
            (glob.glob(f) if '*' in f else [f])
            for f in list(self.args)
        ))

        if len(expanded_csv_files) == 0:
            logger.error('No files provided to CSV source!')
            return

        self.csv_files = expanded_csv_files
        for f in self.csv_files:
            logger.info('Processing %s', f)
            self.process_file(f)

        self.queue.put(SourceDoneCommand())


class DateSource(Source):
    DATE_RESOLUTION = None

    def __init__(self, config: 'Config', queue: PriorityQueue, *args: str):
        super().__init__(config, queue, *args)
        if len(args) != 0 and len(args) != 2:
            logger.critical(
                'date-based source "%s" runs either in live mode '
                'or needs a start and end timestamp',
                self.NAME
            )
            sys.exit(1)

        self.historical = len(args) == 2
        if self.historical:
            try:
                self.start_time = round_time(
                    datetime.fromisoformat(self.args[0]),
                    self.DATE_RESOLUTION
                )
                self.end_time = round_time(
                    datetime.fromisoformat(self.args[1]),
                    self.DATE_RESOLUTION
                )
                if self.start_time >= self.end_time:
                    raise ValueError('end time has to be after start time')
            except Exception:
                logger.critical(
                    'invalid start or end time supplied '
                    'for date-based source "%s"',
                    self.NAME
                )
                sys.exit(1)
