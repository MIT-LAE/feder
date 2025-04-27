import logging
from math import trunc


def logging_setup(debug: bool = False) -> None:
    # Basic logging: assume that all processes will have log files managed by
    # logrotate.
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format='%(asctime)s.%(msecs)03d  %(levelname)s/%(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.getLogger('pika').setLevel(logging.WARNING)
    logging.captureWarnings(True)


def log_counts(logger, name, old_count, increment, digits):
    new_count = old_count + increment
    scale = 10**digits
    if new_count // scale != old_count // scale:
        logger.info('%s ' + name, trunc(new_count / scale) * scale)
    return new_count
