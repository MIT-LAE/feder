from prometheus_client import Counter, Gauge

trajectory_counter = Counter(
    'feder_ingested_trajectories',
    'Trajectories ingested, split by source',
    ['source']
)

batch_time_gauge = Gauge(
    'feder_last_ingested_batch_timestamp_seconds',
    'Timestamp of last trajectory batch ingested, split by source',
    ['source']
)
