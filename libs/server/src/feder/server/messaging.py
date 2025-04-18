import logging

import pika
import pika.credentials

from .config import Config


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
