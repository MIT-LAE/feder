from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
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


@dataclass
class RetrievalFailure:
    message: str
    status_code: int | None = None
    retry_after: timedelta | None = None

    @property
    def retryable(self) -> bool:
        return (
            self.status_code is None or
            self.status_code in (
                requests.codes.request_timeout,
                requests.codes.not_found,
                requests.codes.too_many_requests,
            ) or
            500 <= self.status_code < 600
        )


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
    RETRIEVAL_ATTEMPTS = 5
    RETRY_DELAY = timedelta(minutes=5)
    MAX_RETRY_AFTER = timedelta(minutes=5)
    REQUEST_TIMEOUT = 60

    def __init__(self, config: Config, queue: PriorityQueue, *args, **kwargs):
        # DateSource historically rounded range boundaries. Contrails files are
        # hourly, so silently changing a requested range could lose or add data.
        start_time = kwargs.get('start_time')
        end_time = kwargs.get('end_time')
        if start_time is not None:
            if end_time is None:
                raise ValueError('Contrails historical end time is required')
            for name, value in [('start time', start_time), ('end time', end_time)]:
                if value.minute or value.second or value.microsecond:
                    raise ValueError(f'Contrails historical {name} must be aligned to a whole UTC hour')
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

    def _retrieve_with_retries(self, t: datetime) -> pd.DataFrame | None:
        for attempt in range(1, self.RETRIEVAL_ATTEMPTS + 1):
            result = self._retrieve(t)
            if not isinstance(result, RetrievalFailure):
                return result

            if not result.retryable:
                self.queue.put(SourceErrorCommand(
                    message=f'failed to retrieve ADS-B data for {_format_time(t)}: {result.message}',
                    stop=True,
                ))
                return None

            if attempt == self.RETRIEVAL_ATTEMPTS:
                self.queue.put(SourceErrorCommand(
                    message=(
                        f'failed to retrieve ADS-B data for {_format_time(t)} '
                        f'after {self.RETRIEVAL_ATTEMPTS} attempts: {result.message}'
                    ),
                    stop=True,
                ))
                return None

            delay = self.RETRY_DELAY
            if result.retry_after is not None:
                delay = min(result.retry_after, self.MAX_RETRY_AFTER)
            logger.warning(
                'retrieval of ADS-B data for %s failed (%s); retrying in %s',
                _format_time(t), result.message, delay,
            )
            if self.wait_for(datetime.now(timezone.utc) + delay):
                return None
        return None

    def run_historical(self):
        request_time = self.start_time
        fix_count = 0
        latest_time = datetime(1, 1, 1, tzinfo=timezone.utc)
        while request_time < self.end_time:
            df_or_failure = self._retrieve_with_retries(request_time)
            if df_or_failure is None:
                # A historical run is successful only when every requested
                # hourly file was retrieved and processed. Do not emit DONE.
                return
            df_or_status = df_or_failure
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

    def process_df(self, df) -> Generator[Command, None, None]:
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
        fix_count = 0
        latest_time = datetime(1, 1, 1, tzinfo=timezone.utc)
        while not self.stopped:
            log_time = retrieval_time.strftime('%Y-%m-%dT%H')

            # Wait for the right time to retrieve the next Parquet file.
            # If the process was stopped during the wait, quit immediately.
            logger.info('waiting to retrieve data for %s...', log_time)
            if self.wait_for(retrieval_time + self.config.data_lag(self.SOURCE)):
                break

            # Try retrieving the next Parquet file. A failed file is never
            # skipped: source errors are terminal so downstream state cannot
            # advance past an unknown gap.
            df_or_failure = self._retrieve_with_retries(retrieval_time)
            if df_or_failure is None:
                return
            df_or_status = df_or_failure

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

    def _retrieve(self, t: datetime) -> pd.DataFrame | RetrievalFailure:
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

        try:
            r = requests.get(
                'https://api.contrails.org/v1/adsb/telemetry',
                params={'date': tstr},
                headers={
                    'x-api-key': self.api_key,
                    'transfer-encoding': 'chunked'
                },
                stream=True,
                timeout=self.REQUEST_TIMEOUT,
            )
            if r.status_code != requests.codes.ok:
                logger.error(
                    'HTTP request to Contrails API failed (%s): %s',
                    r.status_code, r.reason
                )
                return RetrievalFailure(
                    f'HTTP {r.status_code}: {r.reason}',
                    status_code=r.status_code,
                    retry_after=_retry_after(r.headers.get('Retry-After')),
                )

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
        except requests.RequestException as exc:
            logger.error('request to Contrails API failed: %s', exc)
            return RetrievalFailure(str(exc))

        data.seek(0)
        try:
            return pd.read_parquet(data, columns=self.COLUMNS)
        except Exception as exc:
            # A truncated response often surfaces only when the Parquet reader
            # validates the footer. Treat it like a connection failure so a
            # historical run cannot silently complete with a missing hour.
            logger.error('failed to decode Contrails API response: %s', exc)
            return RetrievalFailure(f'invalid Parquet response: {exc}')


def _retry_after(
        value: str | None, now: datetime | None = None
) -> timedelta | None:
    if value is None:
        return None
    try:
        return timedelta(seconds=max(0, int(value)))
    except ValueError:
        pass

    try:
        retry_time = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if retry_time.tzinfo is None:
        retry_time = retry_time.replace(tzinfo=timezone.utc)
    if now is None:
        now = datetime.now(timezone.utc)
    return max(retry_time.astimezone(timezone.utc) - now, timedelta(0))


def _format_time(t: datetime) -> str:
    return t.strftime('%Y-%m-%dT%H')
