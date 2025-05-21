from datetime import datetime, timezone, timedelta
import logging
import time

from prometheus_client import start_http_server


logger = logging.getLogger(__name__)


class PrometheusServer:
    def __init__(self, port: int, scrape_interval: timedelta | None = None):
        self.scrape_interval = scrape_interval or timedelta(seconds=60)
        self.last_scrape: datetime = datetime.now(tz=timezone.utc)
        self.server, self.thread = start_http_server(port)
        self.server.set_app(self._scrape_wrapper(self.server.get_app()))

    def shutdown(self):
        self.server.shutdown()
        self.thread.join()

    def wait_for_scrape(self):
        logger.info('waiting for Prometheus scrape')
        while True:
            now = datetime.now(tz=timezone.utc)
            if now > self.last_scrape:
                # Last scrape happened.
                break
            if (now - self.last_scrape) > 2 * self.scrape_interval:
                # Last scrape missed.
                logger.error('Prometheus scrape timeout')
                break
            time.sleep(self.scrape_interval.total_seconds() / 10)

    def _scrape_wrapper(self, orig):
        def inner(*args, **kwargs):
            self.last_scrape = datetime.now(tz=timezone.utc)
            return orig(*args, **kwargs)
        return inner
