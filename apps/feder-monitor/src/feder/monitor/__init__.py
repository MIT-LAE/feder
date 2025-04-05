def main() -> None:
    print("Hello from feder-monitor!")

# Monitor:
#  - heartbeats from sources:
#     - source name
#     - heartbeat timestamp
#     - # of points in staging DB
#     - # of unique flights in staging DB
#     - time of last point in staging DB
#     - # of trajectories sent to ingester since started
#  - heartbeats from ingester
#     - heartbeat timestamp
#  - RabbitMQ queue length for ingester (trajectory.ingester)
#  -
