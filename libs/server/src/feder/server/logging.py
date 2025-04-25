import logging


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


def log_counts(logger, name, old_count, increment, scale):
    new_count = old_count + increment
    if new_count // 10**scale != old_count // 10**scale:
        logger.info('%s ' + name, round(new_count, -scale))
    return new_count
