from datetime import datetime, timedelta
import os

import pytest

from feder_server import Config
from feder_rx.db import DB


TEST_CONFIG = """
[paths]
data-directory = "<PLACEHOLDER>/data"
staging-directory = "<PLACEHOLDER>/staging"
scratch-directory = "<PLACEHOLDER>/scratch"

[rabbitmq]
host = "none"
username = "none"
password = "none"

[mailjet]
api_key = "<fill in API key>"
secret_key = "<fill in secret key>"
from_email = "state-of-feder@mit.edu"
from_name = "State of Feder"
to_email = "state-of-feder@mit.edu"
to_name = "State of Feder"

[ingester]
prometheus-port = 19001

[sources]
# Defaults for all sources.
completion-delay = "15 minutes"
completion-interval = 60
data-lag = 0

[source.contrails-api]
data-lag = "48 hours"
api-key = "<fill in API key>"
prometheus-port = 19002

[source.flightaware]
username = "<fill in username>"
password = "<fill in password>"
prometheus-port = 19003

[source.opensky]
api-key = "<fill in API key>"
prometheus-port = 19004

[source.opensky-state-vectors]
api-key = "<fill in API key>"
prometheus-port = 19005
"""


@pytest.fixture
def config(tmp_path):
    cfg = Config(config_text=TEST_CONFIG)
    os.makedirs(tmp_path / 'data')
    os.makedirs(tmp_path / 'staging')
    os.makedirs(tmp_path / 'scratch')
    cfg.data_directory = tmp_path / 'data'
    cfg.staging_directory = tmp_path / 'staging'
    cfg.scratch_directory = tmp_path / 'scratch'
    return cfg


TEST_NOW = datetime(2025, 4, 1, 12, 0)

# TIMELINE:
#
# -1050  source-0002
# -1020  source-0004
#  -990  source-0002
#  -960  source-0004
#  -930  source-0002
#  -920  source-0004
#  -920  source-0003
#  -900  15-minute deadline
#  -860  source-0003
#  -800  source-0003
#  -125  source-0001
#   -65  source-0001
#    -5  source-0001

def tminus(seconds):
    return TEST_NOW - timedelta(seconds=seconds)

TEST_VALUES = [
    (
        'source-0001', 'ABCDEF', None, 'DUMA', 'DAL1234',
        [
            (tminus(125), 40.1, -94.5, 35000),
            (tminus( 60), 40.2, -94.4, 35000),
            (tminus(  5), 40.3, -9435, 35000),
        ]
    ),
    (
        'source-0002', 'BCDEF0', 'DUMA', 'DUMZ', 'UPS231',
        [
            (tminus(1050), 40.1, -94.5, 35000),
            (tminus( 990), 40.2, -94.4, 35000),
            (tminus( 930), 40.3, -9435, 35000),
        ]
    ),
    (
        'source-0003', 'CDEF01', 'DUMZ', 'DUMA', 'UAL4747',
        [
            (tminus(920), 40.1, -94.5, 35000),
            (tminus(860), 40.2, -94.4, 35000),
            (tminus(800), 40.3, -9435, 35000),
        ]
    ),
    (
        'source-0004', 'DEF012', 'DUMA', None, 'BA1134',
        [
            (tminus(1020), 40.1, -94.5, 35000),
            (tminus( 960), 40.2, -94.4, 35000),
            (tminus( 920), 40.3, -9435, 35000),
        ]
    )
]


@pytest.fixture
def db(config):
    db = DB(config, 'test')

    for (source_id, transponder_id, orig, dest, callsign, points) in TEST_VALUES:
        for (time, lat, lon, alt) in points:
            db.save_position(
                source_id=source_id, transponder_id=transponder_id,
                time=time, orig=orig, dest=dest,
                callsign=callsign, aircraft_type=None,
                lat=lat, lon=lon, alt=alt, alt_gnss=None, heading=None,
                on_ground=False
            )

    return db
