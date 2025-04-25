from abc import abstractmethod
from datetime import datetime, timedelta
import glob
import itertools
import logging
import os
from queue import Queue, Full
import sys
from threading import Thread, Event
from typing import Generator, Any

from feder.common import DataSource
from feder.server import Config, ThreadControl

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

    def __init__(
            self,
            config: Config,
            queue: Queue,
            start_time: datetime | None,
            end_time: datetime | None,
            file_cache: str | None,
            glob_args: list[str]
    ):
        super().__init__()
        self.config = config
        self.queue = queue
        self.start_time = start_time
        self.end_time = end_time
        self.file_cache = file_cache
        self.glob_args = glob_args
        if self.SOURCE is None:
            raise ValueError('unknown source')
        self._check_args()
        self._wait_finished = Event()
        self.control = ThreadControl()
        self.stopped = False

    @classmethod
    def name(cls):
        if cls.NAME is not None:
            return cls.NAME
        return str(cls.SOURCE)

    def stop(self):
        self.stopped = True
        self._wait_finished.set()

    def wait_for(self, t: datetime) -> bool:
        delta = t - datetime.now()
        if delta < timedelta(0):
            return self.stopped
        self._wait_finished.wait(delta.total_seconds())
        self._wait_finished = Event()
        return self.stopped

    def put(self, x: Any) -> None:
        success = False
        while not self.stopped and not success:
            try:
                self.queue.put(x, timeout=0.1)
                success = True
            except Full:
                pass

    def cached_file(self, name) -> str | None:
        if self.file_cache is None:
            return None
        p = os.path.join(self.file_cache, os.path.basename(name))
        return p if os.path.exists(p) else None

    @abstractmethod
    def _check_args(self):
        ...

    @abstractmethod
    def run(self):
        ...


class FileSource(Source):
    def _check_args(self):
        if self.start_time is not None:
            raise ValueError('file source does not use "start-time"')
        if len(self.glob_args) == 0:
            raise ValueError('no file globs provided for file data source')

    @abstractmethod
    def process_file(self, filename: str) -> Generator[Command, None, None]:
        ...

    def run(self):
        expanded_files = list(itertools.chain.from_iterable(
            (sorted(glob.glob(os.path.expanduser(f))) if '*' in f else [f])
            for f in list(self.glob_args)
        ))

        if len(expanded_files) == 0:
            logger.error('No files provided to file source!')
            return

        self.files = expanded_files

        fix_count = 0
        latest_time = datetime(1, 1, 1)
        for f in self.files:
            cached_path = self.cached_file(f)
            if cached_path is not None:
                f = cached_path
            logger.info(
                'Processing %s%s', f,
                ' (from file cache)' if cached_path is not None else ''
            )
            for cmd in self.process_file(f):
                if self.stopped:
                    return
                self.control.check()
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
