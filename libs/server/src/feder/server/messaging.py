import pandas as pd
import pika
import pika.credentials

from .config import Config
from .rabbitmq_pb2 import Trajectory


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
    if any(df.transponder_id != df.transponder_id[0]):
        raise ValueError('inconsistent transponder_id in position fixes')
    if any(df.callsign != df.callsign[0]):
        raise ValueError('inconsistent callsign in position fixes')
    if any(df.aircrafttype != df.aircrafttype[0]):
        raise ValueError('inconsistent aircrafttype in position fixes')
    msg = Trajectory()
    msg.source = source
    msg.id = source_id
    msg.transponder_id = df.transponder_id[0]
    msg.callsign = df.callsign[0]
    msg.aircrafttype = df.aircrafttype[0]
    msg.points.time.extend(df.time)
    msg.points.lon.extend(df.lon)
    msg.points.lat.extend(df.lat)
    msg.points.alt.extend(df.alt)
    msg.points.alt_gnss.extend(df.alt_gnss)
    msg.points.heading.extend(df.heading)
    msg.points.on_ground.extend(df.on_ground)
    return msg
