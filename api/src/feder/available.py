from datetime import date, datetime, timezone
import glob
import itertools
from operator import itemgetter
import os

from feder.common import DB, DataSource


def available_days() -> list[tuple[date, date]]:
    """Get a list of available days in the data directory."""
    data_dir = os.environ.get('FEDER_DATA_DIR')
    if data_dir is None:
        raise ValueError('environment variable FEDER_DATA_DIR must be set')

    days = sorted([
        datetime.strptime(os.path.basename(p)[:8], '%Y-%j').date().toordinal()
        for p in glob.iglob(os.path.join(data_dir, '????/*.sqlite'))
    ])
    ranges = []
    for _, g in itertools.groupby(enumerate(days), lambda x: x[0] - x[1]):
        g = list(g)
        ranges.append((date.fromordinal(g[0][1]), date.fromordinal(g[-1][1])))
    return ranges


def available_sources(day: date) -> set[DataSource]:
    """Return data sources in use on a given day."""
    data_dir = os.environ.get('FEDER_DATA_DIR')
    if data_dir is None:
        raise ValueError('environment variable FEDER_DATA_DIR must be set')

    # Get all data sources from the database.
    return DB(data_dir, day).data_sources()


def available_times(day: date) -> list[tuple[datetime, datetime]]:
    """Get a list of available times for a given day."""
    data_dir = os.environ.get('FEDER_DATA_DIR')
    if data_dir is None:
        raise ValueError('environment variable FEDER_DATA_DIR must be set')

    # Get all min, max timestamps pairs from the database.
    try:
        timestamp_ranges = DB(data_dir, day).timestamp_ranges()
    except FileNotFoundError:
        return []

    # Union the timestamp ranges.
    minval = int(datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc).timestamp())
    maxval = int(datetime.combine(day, datetime.max.time(), tzinfo=timezone.utc).timestamp())
    merged = union_of_ranges([clamp_range(r, minval, maxval) for r in timestamp_ranges])

    # Convert to datetime values and clip to the given day.
    return [
        (datetime.fromtimestamp(r[0], tz=timezone.utc),
         datetime.fromtimestamp(r[1], tz=timezone.utc))
        for r in merged
    ]


def clamp_range(r: tuple[int, int], minval: int, maxval: int) -> tuple[int, int]:
    """Clamp a range to the given min and max values."""
    return (min(maxval, max(minval, r[0])), min(maxval, max(minval, r[1])))


def union_of_ranges(inp: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """Union a list of timestamp ranges."""
    if len(inp) == 0:
        return []

    # Sort ranges by start timestamp.
    inp.sort(key=itemgetter(0))

    # Merge overlapping or contiguous ranges.
    merged = []
    current_start, current_end = inp[0]

    for start, end in inp[1:]:
        if start <= current_end:  # Overlapping or contiguous
            current_end = max(current_end, end)
        else:
            merged.append((current_start, current_end))
            current_start, current_end = start, end

    merged.append((current_start, current_end))
    return merged
