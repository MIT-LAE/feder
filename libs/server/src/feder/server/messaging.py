import pandas as pd

from feder.server.rabbitmq_pb2 import *


class TrajectoryMessage:
    def __init__(self, source: str, source_id: str, df: pd.DataFrame):
        ...
