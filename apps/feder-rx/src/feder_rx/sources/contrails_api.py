from datetime import datetime, timedelta, timezone
import gc
from io import BytesIO
import logging
from queue import PriorityQueue
from typing import Generator

import pandas as pd
import requests

from feder_common import DataSource
from feder_server import Config

from . import DateSource
from ..commands import (
    Command, SourceErrorCommand, SourceDoneCommand,
    SourcePositionCommand, BatchSourcePositionCommand,
    EndOfDayCommand
)
from ..utils import ceil_time


logger = logging.getLogger(__name__)


class ContrailsAPISource(DateSource):
    SOURCE = DataSource.CONTRAILS_API
    BATCH_SIZE = 100

    # Columns to read from Contrails API Parquet files.
    COLUMNS = [
        'flight_id', 'icao_address', 'timestamp', 'callsign',
        'departure_airport_icao', 'arrival_airport_icao', 'aircraft_type_icao',
        'latitude', 'longitude', 'altitude_baro', 'altitude_gnss'
    ]

    # The Contrails API provides hourly ADS-B files.
    DATE_RESOLUTION = 'h'
    DATE_INTERVAL = timedelta(hours=1)

    def __init__(self, config: Config, queue: PriorityQueue, *args, **kwargs):
        super().__init__(config, queue, *args, **kwargs)
        self.api_key = config.credentials(self.SOURCE)['api_key']
        self.bounds = config.bounds(self.SOURCE)
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
            case requests.codes.not_found:
                logger.info('data not available yet...')
                return True
            case _:
                self.queue.put(SourceErrorCommand(
                    message=(
                        f'failed to retrieve ADS-B Parquet file for {_format_time(t)}' +
                        (' (waiting 5 minutes to try again...)' if retry else '')
                    ),
                    stop=not retry
                ))
                return retry

    def run_historical(self):
        request_time = self.start_time
        fix_count = 0
        latest_time = datetime(1, 1, 1, tzinfo=timezone.utc)
        while request_time <= self.end_time:
            df_or_status = self._retrieve(request_time)
            if isinstance(df_or_status, int):
                # TODO: CHECK RETURN VALUE AND WAIT FOR FIVE MINUTES IF WE'RE
                # NOT QUITTING RIGHT AWAY.
                self.retrieve_error(request_time, df_or_status)
                break
            for cmd in self.process_df(df_or_status):
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

            # This is the last file for the day.
            if request_time.hour == 23:
                logger.info(
                    'writing end of day command: %s',
                    request_time.strftime('%Y-%j')
                )
                self.put(EndOfDayCommand(request_time.date()))

            request_time += self.DATE_INTERVAL

        logger.info('total position fixes from source: %s', fix_count)
        self.put(SourceDoneCommand(latest_time))

    def process_df(self, df) -> Generator[Command]:
        # Helper for value conversion.
        def n(x, xform):
            return None if pd.isna(x) or x == '' else xform(x)

        # Ignore records with no callsign!
        filter = ~df.callsign.isna() & ~df.flight_id.isna() & ~df.timestamp.isna()

        # Spatial filtering if required.
        if self.bounds is not None:
            min_lon, max_lon, min_lat, max_lat = self.bounds
            filter &= (
                (df.longitude >= min_lon) & (df.longitude <= max_lon) &
                (df.latitude >= min_lat) & (df.latitude <= max_lat)
            )

        unfiltered_rows = len(df)
        df = df[filter]
        filtered_rows = len(df)
        if len(df) == 0:
            logger.warning('no data from Contrails API data for this time!')
            return
        logger.info(
            'retrieved %s rows from Contrails API, filtered to %s rows',
            unfiltered_rows, filtered_rows
        )

        # NOTE: Contrails API returns rows in *reverse* time order so let's be
        # defensive and reverse them to get them in the right order if that's
        # the case!
        if df.timestamp.iloc[0] > df.timestamp.iloc[-1]:
            df = df[::-1]

        for tup in df.itertuples(index=False):
            # One source position command per row.
            self._source_ids.append(tup.flight_id)
            self._transponder_ids.append(tup.icao_address)
            self._times.append(
                tup.timestamp.to_pydatetime().replace(tzinfo=timezone.utc)
            )
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
        retrieval_time = (
            datetime.now(timezone.utc) - self.config.data_lag(self.SOURCE)
        )
        retrieval_time = ceil_time(retrieval_time, 'h')
        retries = 0
        fix_count = 0
        latest_time = datetime(1, 1, 1, tzinfo=timezone.utc)
        while not self.stopped:
            log_time = retrieval_time.strftime('%Y-%m-%dT%H')

            # Wait for the right time to retrieve the next Parquet file.
            # If the process was stopped during the wait, quit immediately.
            logger.info('waiting to retrieve data for %s...', log_time)
            if self.wait_for(retrieval_time + self.config.data_lag(self.SOURCE)):
                break

            # Try retrieving the next file.
            df_or_status = self._retrieve(retrieval_time)

            # If the retrieval failed, we try again for the same file in 5
            # minutes. After five failed attempts, we skip the file and wait
            # for the next one.
            if isinstance(df_or_status, int):
                if retries >= 5:
                    self.queue.put(SourceErrorCommand(
                        message='skipping time after five retrieval attempts',
                        stop=False
                    ))
                    retries = 0
                    retrieval_time += self.DATE_INTERVAL
                else:
                    # retrieve_error returns False if the error is
                    # unrecoverable.
                    if not self.retrieve_error(retrieval_time, df_or_status, retry=True):
                        self.queue.put(SourceErrorCommand(
                            message=f'unrecoverable error retrieving data: {df_or_status}',
                            stop=True
                        ))
                        break
                    if self.wait_for(datetime.now(timezone.utc) + timedelta(minutes=5)):
                        break
                    retries += 1
                continue

            logger.info('processing data for %s...', log_time)

            # It would be really good here to be able to stream data to a
            # temporary file and then use PyArrow to read the file in batches
            # to help keep memory consumption down. However, that's not
            # possible, because of the backwards order of the timestamps in
            # the Contrails API files, so we have to read the whole dataset as
            # a Pandas DataFrame and process it in one go.
            for cmd in self.process_df(df_or_status):
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

            # This is the last file for the day.
            if retrieval_time.hour == 23:
                logger.info(
                    'writing end of day command: %s',
                    retrieval_time.strftime('%Y-%j')
                )
                self.put(EndOfDayCommand(retrieval_time.date()))

            retries = 0
            retrieval_time += self.DATE_INTERVAL

            # Don't keep the dataframe from the last timestep hanging around
            # consuming memory while wee wait for the next file to be ready to
            # retrieve.
            df_or_status = None
            gc.collect()

        # The only way we get here under normal circumstances is if the
        # receiver process is interrupted by some error condition, either an
        # external signal or a failure to retrieve ADS-B data after repeated
        # retries.

    def _retrieve(self, t: datetime) -> pd.DataFrame | int:
        # ISO 8601 (UTC)
        tstr = _format_time(t)

        cached_path = self.cached_file(f'{tstr}.pq')
        if cached_path is not None:
            logger.info('using cached Spire ADS-B data for %s', tstr)
            try:
                return pd.read_parquet(cached_path)
            except Exception:
                logger.error('failed to read cached data - retrieving again...')

        logger.info('retrieving Spire ADS-B data for %s', tstr)

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

        # Retrieve data, simultaneously saving to cache file if required.
        save_fp = None
        data = BytesIO()
        try:
            if self.file_cache is not None:
                save_fp = open(self.cache_path(f'{tstr}.pq'), 'wb')

            for chunk in r.iter_content(chunk_size=128):
                data.write(chunk)
                if save_fp is not None:
                    save_fp.write(chunk)
        finally:
            if save_fp is not None:
                save_fp.close()

        data.seek(0)
        return pd.read_parquet(data, columns=self.COLUMNS)


def _format_time(t: datetime) -> str:
    return t.strftime('%Y-%m-%dT%H')
