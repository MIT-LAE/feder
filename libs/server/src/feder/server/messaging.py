from collections import Counter
import logging

import numpy as np
import pandas as pd
import pika
import pika.credentials

from .config import Config
from .rabbitmq_pb2 import Trajectory


logger = logging.getLogger(__name__)


# Virtual host and exchange names for RabbitMQ processing.
RMQ_VIRTUAL_HOST = 'flight-data'
RMQ_TRAJECTORY_EXCHANGE = 'trajectory'
RMQ_MONITOR_EXCHANGE = 'monitor'


def rmq_parameters(config: 'Config'):
    creds = pika.credentials.PlainCredentials(
        username=config.rabbitmq_username,
        password=config.rabbitmq_password
    )
    return pika.ConnectionParameters(
        host=config.rabbitmq_host,
        port=config.rabbitmq_port,
        virtual_host=RMQ_VIRTUAL_HOST,
        credentials=creds
    )


def build_trajectory_message(
        source: str, source_id: str, df: pd.DataFrame
) -> Trajectory:
    try:
        _single_value_column_check(df, 'transponder_id')
        _single_value_column_check(df, 'callsign')
        _single_value_column_check(df, 'aircrafttype')
        msg = Trajectory()
        msg.source = source
        msg.id = source_id
        msg.transponder_id = df.transponder_id[0] or ''
        msg.callsign = df.callsign[0] or ''
        msg.aircrafttype = df.aircrafttype[0] or ''
        msg.points.time.extend(df.time)
        msg.points.lon.extend(df.lon)
        msg.points.lat.extend(df.lat)
        msg.points.alt.extend(_substitute_none(df.alt))
        msg.points.alt_gnss.extend(_substitute_none(df.alt_gnss))
        msg.points.heading.extend(_substitute_none(df.heading))
        msg.points.on_ground.extend(df.on_ground)
        return msg
    except Exception as e:
        print('OOPS')
        print(df)


# TODO: Make this better.
def parse_trajectory_message(traj: Trajectory) -> tuple[str, str, pd.DataFrame]:
    df = pd.DataFrame(dict(
        transponder_id=traj.transponder_id,
        callsign=traj.callsign,
        aircrafttype=traj.aircrafttype,
        time=traj.time,
        lon=traj.lon,
        lat=traj.lat,
        alt=_replace_none(traj.alt),
        alt_gnss=_replace_none(traj.alt_gnss),
        on_ground=_replace_none(traj.on_ground)
    ))
    _single_value_column_check(df, 'transponder_id')
    _single_value_column_check(df, 'callsign')
    _single_value_column_check(df, 'aircrafttype')
    return traj.source, traj.source_id, df


def _single_value_column_check(df: pd.DataFrame, column_name: str):
    if len(set(df[column_name])) != 1:
        msg = f'inconsistent {column_name} in position fixes: {list(df[column_name])}'
        logger.warn(msg)

        # Just take a majority vote.
        df[column_name] = Counter(df[column_name]).most_common(1)[0][0]


def _substitute_none(xs):
    return np.where(pd.isna(xs), -999999, xs)


def _replace_none(xs):
    return [x if x >= -100000 else None for x in xs]
