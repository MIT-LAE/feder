from datetime import date, datetime
import glob
import os
import sqlite3

from feder import available_days, available_times


def test_available_days():
    day_ranges = available_days()

    data_dir = os.environ['FEDER_DATA_DIR']
    check_days = set(
        datetime.strptime(os.path.basename(p)[:8], '%Y-%j').date()
        for p in glob.glob(os.path.join(data_dir, '*/*.sqlite'))
    )

    for start_range, end_range in day_ranges:
        for d in range(start_range.toordinal(), end_range.toordinal() + 1):
            dt = datetime.fromordinal(d).date()
            assert dt in check_days
            check_days.remove(dt)
    assert len(check_days) == 0, f"Missing days: {check_days}"


def test_available_times():
    d = date(2025, 5, 22)
    time_ranges = available_times(d)

    data_dir = os.environ['FEDER_DATA_DIR']
    db_file = os.path.join(data_dir, '2025', '2025-142.sqlite')
    conn = sqlite3.connect(db_file)

    def count_rows(start: datetime, end: datetime) -> int:
        cur = conn.cursor()
        cur.execute(
            """SELECT COUNT(*)
                 FROM trajectory_index
                WHERE min_timestamp >= ? AND max_timestamp < ?""",
            (start.timestamp(), end.timestamp())
        )
        return cur.fetchone()[0]

    previous_end = None
    for start, end in time_ranges:
        assert count_rows(start, end) > 0
        if previous_end is not None:
            assert count_rows(previous_end, start) == 0
        previous_end = start
