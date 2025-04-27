from datetime import datetime, timedelta
from io import BytesIO
import logging
from queue import PriorityQueue
from typing import Generator

import numpy as np
import pandas as pd
import requests

from feder.common import DataSource
from feder.server import Config

from . import DateSource
from ..commands import (
    Command, SourceErrorCommand, SourceDoneCommand,
    SourcePositionCommand, BatchSourcePositionCommand
)
from ..utils import round_time


logger = logging.getLogger(__name__)


class ContrailsAPISource(DateSource):
    SOURCE = DataSource.CONTRAILS_API
    BATCH_SIZE = 100

    # The Contrails API provides hourly ADS-B files.
    DATE_RESOLUTION = 'h'
    DATE_INTERVAL = timedelta(hours=1)

    def __init__(self, config: Config, queue: PriorityQueue, *args, **kwargs):
        super().__init__(config, queue, *args, **kwargs)
        self.api_key = config.credentials(self.SOURCE)['api_key']
        self._clear()

        # There's no way to check that the credentials are OK at this point.
        # We just need to go ahead and try the ADS-B endpoint and generate a
        # source error command if it fails.

    def _clear(self):
        self._source_ids = []
        self._transponder_ids = []
        self._times = []
        self._origs = []
        self._dests = []
        self._callsigns = []
        self._aircraft_types = []
        self._lats = []
        self._lons = []
        self._alts = []
        self._alts_gnss = []
        self._headings = []
        self._on_grounds = []
        self._nrows = 0

    def run(self):
        if self.historical:
            self.run_historical()
        else:
            self.run_live()

    def retrieve_error(
            self, t: datetime, status_code: int, retry: bool = False
    ) -> bool:
        match status_code:
            case requests.codes.unauthorized:
                self.queue.put(SourceErrorCommand(
                    message='invalid credentials for Contrails API',
                    stop=True
                ))
                return False
            case _:
                self.queue.put(SourceErrorCommand(
                    message=(
                        f'failed to retrieve ADS-B Parquet file for {_format_time(t)}'
                        (' (waiting 5 minutes to try again...)' if retry else '')
                    ),
                    stop=not retry
                ))
                return not retry

    def run_historical(self):
        request_time = self.start_time
        fix_count = 0
        latest_time = datetime(1, 1, 1)
        while request_time <= self.end_time:
            df_or_status = self._retrieve(request_time)
            if isinstance(df_or_status, int):
                # TODO: CHECK RETURN VALUE AND WAIT FOR FIVE MINUTES IF WE'RE
                # NOT QUITTING RIGHT AWAY.
                self.retrieve_error(request_time, df_or_status)
                break
            df = df_or_status
            for cmd in self.process_df(df):
                if self.stopped:
                    return
                self.control.check()
                match cmd:
                    case SourcePositionCommand():
                        fix_count += 1
                        latest_time = max(latest_time, cmd.time)
                    case BatchSourcePositionCommand():
                        fix_count += len(cmd.source_ids)
                        latest_time = max(latest_time, *cmd.times)
                self.put(cmd)

            request_time += self.DATE_INTERVAL

        logger.info('Total position fixes from source: %s', fix_count)
        self.put(SourceDoneCommand(latest_time))

    def process_df(self, df) -> Generator[Command, None, None]:
        # Helper for value conversion.
        def n(x, xform):
            return None if pd.isna(x) or x == '' else xform(x)

        # Ignore records with no callsign!
        # NOTE: Contrails API returns rows in *reverse* time order...
        for tup in df[~df.callsign.isna()][::-1].itertuples(index=False):
            # One source position command per row.
            self._source_ids.append(tup.flight_id)
            self._transponder_ids.append(tup.icao_address)
            self._times.append(tup.timestamp.to_pydatetime())
            self._callsigns.append(n(tup.callsign, str))
            self._origs.append(n(tup.departure_airport_icao, str))
            self._dests.append(n(tup.arrival_airport_icao, str))
            self._aircraft_types.append(n(tup.aircraft_type_icao, str))
            self._lats.append(float(tup.latitude))
            self._lons.append(float(tup.longitude))
            self._alts.append(n(tup.altitude_baro, float))
            self._alts_gnss.append(n(tup.altitude_gnss, float))
            self._headings.append(None)
            self._on_grounds.append(False)
            self._nrows += 1

            if self._nrows == self.BATCH_SIZE:
                yield BatchSourcePositionCommand(
                    self._source_ids, self._transponder_ids, self._times,
                    self._origs, self._dests, self._callsigns,
                    self._aircraft_types,
                    self._lats, self._lons, self._alts, self._alts_gnss,
                    self._headings, self._on_grounds
                )
                self._clear()

        if self._nrows > 0:
            yield BatchSourcePositionCommand(
                self._source_ids, self._transponder_ids, self._times,
                self._origs, self._dests, self._callsigns,
                self._aircraft_types,
                self._lats, self._lons, self._alts, self._alts_gnss,
                self._headings, self._on_grounds
            )
            self._clear()

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
            df_or_status = self._retrieve(retrieval_time)

            # If the retrieval failed, we try again for the same file in 5
            # minutes.
            if isinstance(df_or_status, int):
                if retries >= 5:
                    self.queue.put(SourceErrorCommand(
                        message='stopping after five retrieval attempts',
                        stop=True
                    ))
                    break

                # retrieve_error returns False if the error is unrecoverable.
                if not self.retrieve_error(retrieval_time, retry=True):
                    break
                if self.wait_for(datetime.now() + timedelta(minutes=5)):
                    break
                retries += 1
                continue

            # TODO: FIX THIS
            self.process_df(df)
            retries = 0
            retrieval_time += self.DATE_INTERVAL

        # The only way we get here under normal circumstances is if the
        # receiver process is interrupted by some error condition, either an
        # external signal or a failure to retrieve ADS-B data after repeated
        # retries.
        self.queue.put(SourceErrorCommand('unknown error', stop=True))

    def _retrieve(self, t: datetime) -> pd.DataFrame | int:
        # ISO 8601 (UTC)
        tstr = _format_time(t)

        cached_path = self.cached_file(f'{tstr}.pq')
        if cached_path is not None:
            logger.info('Using cached Spire ADS-B data for %s', tstr)
            return pd.read_parquet(cached_path)

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
            return r.status_code

        # if self.file_cache is not None:
        #     # TODO: Save retrieved file to cache and read.
        #     ...
        # else:
        return pd.read_parquet(BytesIO(r.content))


def _format_time(t: datetime) -> str:
    return t.strftime('%Y-%m-%dT%H')
