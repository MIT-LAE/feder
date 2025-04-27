import logging

from feder.server import log_counts


logger = logging.getLogger(__name__)


def test_log_counts(caplog):
    caplog.set_level(logging.INFO)
    log_counts(logger, 'test', 80, 40, 2)
    assert '100 test' in caplog.text
