from prometheus_client import Counter, Gauge

from feder import get_feder_version


error_counter = Counter('feder_error_count', 'Feder errors', ['source'])

version_gauge = Gauge('feder_version', 'Feder software version', ['version'])

def set_version():
    version_gauge.labels(version=get_feder_version()).set(1)
