import os

import pytest

from feder.server.config import Config
from feder.rx.staging import DB


TEST_CONFIG = """
[paths]
data-directory = "<PLACEHOLDER>"
scratch-directory = "<PLACEHOLDER>"

[rabbitmq]
host = "none"
username = "none"
password = "none"

[monitoring]
heartbeat-interval = 30
from-email = "feder-monitor@mit.edu"
from-name = "Feder Monitoring"
to-email = "ian@skybluetrades.net"
to-name = "Ian Ross"
mail-backend = "dummy" # for development, normally, default = "mailjet"
mailjet-api-key = "<fill in API key>"
mailjet-secret-key = "<fill in secret key>"

[sources]
# Defaults for all sources.
completion-delay = "15 minutes"
completion-interval = 60
data-lag = 0

[source.contrails-api]
enabled = false
data-lag = "48 hours"
api-key = "<fill in API key>"

[source.flightaware]
enabled = false
username = "<fill in username>"
password = "<fill in password>"

[source.opensky]
enabled = false
api-key = "<fill in API key>"

[source.opensky-state-vectors]
enabled = false
api-key = "<fill in API key>"

[source.csv]
enabled = true
"""


@pytest.fixture
def config(tmp_path):
    cfg = Config(config_text=TEST_CONFIG)
    os.makedirs(tmp_path / 'data')
    os.makedirs(tmp_path / 'scratch')
    cfg.data_directory = tmp_path / 'data'
    cfg.scratch_directory = tmp_path / 'scratch'
    return cfg


TEST_NOW = 1743184519

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

TEST_VALUES = [
    (
        'source-0001', 'ABCDEF', 'DAL1234',
        [
            (TEST_NOW - 125, 40.1, -94.5, 35000),
            (TEST_NOW -  60, 40.2, -94.4, 35000),
            (TEST_NOW -   5, 40.3, -9435, 35000),
        ]
    ),
    (
        'source-0002', 'BCDEF0', 'UPS231',
        [
            (TEST_NOW - 1050, 40.1, -94.5, 35000),
            (TEST_NOW -  990, 40.2, -94.4, 35000),
            (TEST_NOW -  930, 40.3, -9435, 35000),
        ]
    ),
    (
        'source-0003', 'CDEF01', 'UAL4747',
        [
            (TEST_NOW - 920, 40.1, -94.5, 35000),
            (TEST_NOW - 860, 40.2, -94.4, 35000),
            (TEST_NOW - 800, 40.3, -9435, 35000),
        ]
    ),
    (
        'source-0004', 'DEF012', 'BA1134',
        [
            (TEST_NOW - 1020, 40.1, -94.5, 35000),
            (TEST_NOW -  960, 40.2, -94.4, 35000),
            (TEST_NOW -  920, 40.3, -9435, 35000),
        ]
    )
]


@pytest.fixture
def db(config):
    sql = """
      INSERT INTO fixes
        (source_id, transponder_id, callsign, time, lat, lon, alt)
      VALUES
        (?, ?, ?, ?, ?, ?, ?)
    """

    db = DB(config, 'test')

    cur = db.conn.cursor()
    for (source_id, transponder_id, callsign, points) in TEST_VALUES:
        for (time, lat, lon, alt) in points:
            cur.execute(
                sql, (source_id, transponder_id, callsign, time, lat, lon, alt)
            )

    db.conn.commit()

    return db
