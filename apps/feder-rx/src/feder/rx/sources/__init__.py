from abc import abstractmethod
from datetime import datetime, timedelta
import glob
import itertools
import logging
from queue import Queue, Full
import sys
from threading import Thread, Event
from typing import Generator, Any

from feder.common import DataSource
from feder.server import Config

from ..utils import round_time
from ..commands import (
    Command,
    SourcePositionCommand, BatchSourcePositionCommand,
    SourceDoneCommand
)


logger = logging.getLogger(__name__)


class Source(Thread):
    SOURCE: DataSource | None = None
    NAME: str | None = None
    BATCH_SIZE: int = 1

    def __init__(self, config: Config, queue: Queue, *args: str):
        super().__init__()
        self.config = config
        self.queue = queue
        self.args = args
        if self.SOURCE is None:
            raise ValueError('unknown source')
        self.wait_finished = Event()
        self.stopped = False

    @classmethod
    def name(cls):
        if cls.NAME is not None:
            return cls.NAME
        return str(cls.SOURCE)

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

    def put(self, x: Any) -> None:
        success = False
        while not self.stopped and not success:
            try:
                self.queue.put(x, timeout=0.1)
                success = True
            except Full:
                pass

    @abstractmethod
    def run(self):
        ...


class FileSource(Source):
    @abstractmethod
    def process_file(self, filename: str) -> Generator[Command, None, None]:
        ...

    def run(self):
        expanded_csv_files = list(itertools.chain.from_iterable(
            (sorted(glob.glob(f)) if '*' in f else [f])
            for f in list(self.args)
        ))

        if len(expanded_csv_files) == 0:
            logger.error('No files provided to CSV source!')
            return

        self.csv_files = expanded_csv_files

        fix_count = 0
        latest_time = datetime(1, 1, 1)
        for f in self.csv_files:
            logger.info('Processing %s', f)
            for cmd in self.process_file(f):
                if self.stopped:
                    return
                match cmd:
                    case SourcePositionCommand():
                        fix_count += 1
                        latest_time = max(latest_time, cmd.time)
                    case BatchSourcePositionCommand():
                        fix_count += len(cmd.source_ids)
                        latest_time = max(latest_time, *cmd.times)
                self.put(cmd)

        logger.info('Total position fixes from source: %s', fix_count)
        self.put(SourceDoneCommand(latest_time))


class DateSource(Source):
    DATE_RESOLUTION = None

    def __init__(self, config: 'Config', queue: Queue, *args: str):
        super().__init__(config, queue, *args)
        if len(args) != 0 and len(args) != 2:
            logger.critical(
                'date-based source "%s" runs either in live mode '
                'or needs a start and end timestamp',
                str(self.SOURCE)
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
                    str(self.SOURCE)
                )
                sys.exit(1)
