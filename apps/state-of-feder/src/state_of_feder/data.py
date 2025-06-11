from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

from feder_server import Config
from feder import available_days


@dataclass
class EmailData:
    report_time: datetime
    available_dates: list[tuple[date, date ]]
    small_sizes: list[tuple[date, int]]


def retrieve_data(cfg: Config) -> EmailData:
    return EmailData(
        report_time=datetime.now(tz=timezone.utc),
        available_dates=available_days(),
        small_sizes=file_sizes(cfg)[:5]
    )


def file_sizes(cfg: Config) -> list[tuple[date, int]]:
    """Return a dictionary with file sizes in the data directory."""

    data_dir = Path(cfg.data_directory)
    sizes = [
        (datetime.strptime(file.stem, '%Y-%j').date(), file.stat().st_size)
        for file in data_dir.glob('**/*')
        if file.is_file()
    ]
    return sorted(sizes, key=lambda x: x[1])
