from datetime import datetime, timedelta
from io import BytesIO
import logging
from queue import PriorityQueue
from typing import Any, cast

import numpy as np
import pandas as pd
import requests

from feder.common import DataSource
from feder.server import Config

from . import DateSource
from ..commands import SourceErrorCommand, SourcePositionCommand
from ..utils import round_time


logger = logging.getLogger(__name__)


class ContrailsAPISource(DateSource):
    SOURCE = DataSource.CONTRAILS_API

    # The Contrails API provides hourly ADS-B files.
    DATE_RESOLUTION = 'h'
    DATE_INTERVAL = timedelta(hours=1)

    def __init__(self, config: Config, queue: PriorityQueue, *args: str):
        super().__init__(config, queue, *args)
        self.api_key = config.credentials(self.SOURCE)['api_key']

        # There's no way to check that the credentials are OK at this point.
        # We just need to go ahead and try the ADS-B endpoint and generate a
        # source error command if it fails.

    def run(self):
        if self.historical:
            self.run_historical()
        else:
            self.run_live()

    def retrieve_error(self, t: datetime, retry: bool = False) -> None:
        self.queue.put(SourceErrorCommand(
            f'failed to retrieve ADS-B Parquet file for {_format_time(t)}' +
            (' (waiting 5 minutes to try again...)' if retry else '')
        ))

    def run_historical(self):
        request_time = self.start_time
        while request_time < self.end_time:
            df = self.retrieve(request_time)
            if df is None:
                self.retrieve_error(request_time)
                break
            self.process_df(df)
            request_time += self.DATE_INTERVAL

    def process_df(self, df):
        for tup in df.itertuples(index=False):
            # Needed to suppress spurious pyright messages.
            tup: Any = cast(Any, tup)

            self.queue.put(SourcePositionCommand(
                source_id=tup.flight_id,
                transponder_id=tup.icao_address,
                time=tup.timestamp,
                callsign=tup.callsign,
                aircraft_type=tup.aircraft_type_icao,
                lat=tup.latitude,
                lon=tup.longitude,
                alt=tup.altitude_baro,
                alt_gnss=None if np.isnan(tup.altitude_gnss) else int(tup.altitude_gnss),
                heading=None,
                on_ground=False
            ))

    def run_live(self):
        retrieval_time = datetime.now() - self.config.data_lag(self.SOURCE)
        retrieval_time = round_time(retrieval_time - self.DATE_INTERVAL, 'h')
        retries = 0
        while not self.stopped:
            # Wait for the right time to retrieve the next Parquet file.
            # If the process was stopped during the wait, quit immediately.
            if self.wait_for(retrieval_time):
                break

            # Try retrieving the next file.
            df = self.retrieve(retrieval_time)

            # If the retrieval failed, we try again for the same file in 5
            # minutes.
            if df is None:
                if retries >= 5:
                    self.queue.put(SourceErrorCommand(
                       'failed to retrieve data after five attempts: exiting'
                    ))
                    break

                self.retrieve_error(retrieval_time, retry=True)
                if self.wait_for(datetime.now() + timedelta(minutes=5)):
                    break
                retries += 1
                continue

            self.process_df(df)
            retries = 0
            retrieval_time += self.DATE_INTERVAL

        # The only way we get here under normal circumstances is if the
        # receiver process is interrupted by some error condition, either an
        # external signal or a failure to retrieve ADS-B data after repeated
        # retries.
        self.queue.put(SourceErrorCommand('unknown error', stop=True))

    def retrieve(self, t: datetime) -> pd.DataFrame | None:
        # ISO 8601 (UTC)
        tstr = _format_time(t)
        logger.info('Retrieving Spire ADS-B data for %s', tstr)

        r = requests.get(
            'https://api.contrails.org/v1/adsb/telemetry',
            params={'date': tstr},
            headers={
                'x-api-key': self.api_key,
                'transfer-encoding': 'chunked'
            },
            stream=True
        )
        if r.status_code != requests.codes.ok:
            logger.error(
                'HTTP request to Contrails API failed (%s): %s',
                r.status_code, r.reason
            )
            return None

        return pd.read_parquet(BytesIO(r.content))


def _format_time(t: datetime) -> str:
    return t.strftime('%Y-%m-%dT%H')
