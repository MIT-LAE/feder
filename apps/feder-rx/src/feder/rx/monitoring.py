from prometheus_client import Counter, Gauge

fix_counter = Counter(
    'feder_fixes',
    'Position fixes received, split by source',
    ['source']
)

last_completion_fix_counter = Counter(
    'feder_fixes_last_completion',
    'Position fixes received at last trajectory completion cycle, split by source',
    ['source']
)

trajectory_counter = Counter(
    'feder_trajectories_sent',
    'Trajectories sent to ingester, split by source',
    ['source']
)

latest_fix_time_gauge = Gauge(
    'feder_latest_fix_timestamp_seconds',
    'Timestamp of latest position fix received, split by source',
    ['source']
)

last_completion_fix_time_gauge = Gauge(
    'feder_last_completion_fix_timestamp_seconds',
    'Timestamp of latest position fix received at time of last trajectory completion cycle, split by source',
    ['source']
)

last_completion_time_gauge = Gauge(
    'feder_last_completion_timestamp_seconds',
    'Timestamp of last trajectory completion cycle, split by source',
    ['source']
)

error_counter = Counter(
    'feder_error_count',
    'Receiver errors',
    ['source']
)
