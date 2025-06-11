import logging
import os

import click

from feder_server import logging_setup, Config

from .data import retrieve_data
from .email import send_email


__version__ = '0.1.11'


logger = logging.getLogger(__name__)


@click.command()
@click.option(
    '--debug/--no-debug', default=False,
    help='Set logging level to DEBUG.'
)
@click.option(
    '--config', '-c',
    help='Path to Feder configuration file'
)
def run(debug: bool, config: str | None) -> None:
    logging_setup(debug)

    cfg = Config(config)
    os.environ['FEDER_DATA_DIR'] = cfg.data_directory

    send_email(cfg, retrieve_data(cfg))


if __name__ == '__main__':
    run()
