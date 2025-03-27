import logging


def logging_setup(debug: bool = False) -> None:
    # Basic logging: assume that all processes will have log files managed by
    # logrotate.
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format='%(asctime)s.%(msecs)03d  %(levelname)s/%(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.captureWarnings(True)
