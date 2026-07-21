from datetime import datetime, timedelta, timezone
from queue import PriorityQueue

import pandas as pd
import pytest

from feder_rx import _parse_utc_time
from feder_rx.commands import SourceDoneCommand, SourceErrorCommand
from feder_rx.sources.contrails_api import (
    ContrailsAPISource, RetrievalFailure, _retry_after,
)


UTC = timezone.utc


def _source(config, start, end):
    return ContrailsAPISource(
        config, PriorityQueue(), start_time=start, end_time=end,
        file_cache=None, glob_args=[],
    )


def _empty_data_frame():
    return pd.DataFrame({
        'flight_id': [], 'icao_address': [], 'timestamp': [], 'callsign': [],
        'departure_airport_icao': [], 'arrival_airport_icao': [],
        'aircraft_type_icao': [], 'latitude': [], 'longitude': [],
        'altitude_baro': [], 'altitude_gnss': [],
    })


def test_historical_contrails_range_is_hourly_half_open(config, monkeypatch):
    source = _source(
        config,
        datetime(2025, 4, 1, 12, tzinfo=UTC),
        datetime(2025, 4, 1, 14, tzinfo=UTC),
    )
    requested = []
    monkeypatch.setattr(
        source, '_retrieve_with_retries',
        lambda value: requested.append(value) or _empty_data_frame(),
    )

    source.run_historical()

    assert requested == [
        datetime(2025, 4, 1, 12, tzinfo=UTC),
        datetime(2025, 4, 1, 13, tzinfo=UTC),
    ]
    assert isinstance(source.queue.get_nowait(), SourceDoneCommand)


@pytest.mark.parametrize('value', [
    datetime(2025, 4, 1, 12, 1, tzinfo=UTC),
    datetime(2025, 4, 1, 12, 0, 1, tzinfo=UTC),
    datetime(2025, 4, 1, 12, 0, 0, 1, tzinfo=UTC),
])
def test_historical_contrails_range_requires_whole_utc_hours(config, value):
    with pytest.raises(ValueError, match='whole UTC hour'):
        _source(config, value, datetime(2025, 4, 1, 13, tzinfo=UTC))


def test_timestamp_offsets_are_converted_to_utc_before_validation():
    assert _parse_utc_time('2025-04-01T14:00:00+02:00') == datetime(
        2025, 4, 1, 12, tzinfo=UTC,
    )


def test_retryable_failures_retry_five_times_with_bounded_retry_after(config, monkeypatch):
    source = _source(
        config,
        datetime(2025, 4, 1, 12, tzinfo=UTC),
        datetime(2025, 4, 1, 13, tzinfo=UTC),
    )
    attempts = []
    waits = []
    monkeypatch.setattr(
        source, '_retrieve',
        lambda _t: attempts.append(1) or RetrievalFailure(
            'HTTP 429', status_code=429, retry_after=timedelta(hours=1),
        ),
    )
    monkeypatch.setattr(source, 'wait_for', lambda when: waits.append(when) or False)

    assert source._retrieve_with_retries(source.start_time) is None
    assert len(attempts) == 5
    assert len(waits) == 4
    now = datetime.now(UTC)
    for wait in waits:
        assert timedelta(minutes=4, seconds=59) <= wait - now <= timedelta(minutes=5, seconds=1)
    assert isinstance(source.queue.get_nowait(), SourceErrorCommand)


@pytest.mark.parametrize('status', [408, 404, 429, 500, 503])
def test_timeout_missing_rate_limit_and_server_failures_are_retryable(
        config, monkeypatch, status,
):
    source = _source(
        config,
        datetime(2025, 4, 1, 12, tzinfo=UTC),
        datetime(2025, 4, 1, 13, tzinfo=UTC),
    )
    attempts = []
    monkeypatch.setattr(source, 'RETRIEVAL_ATTEMPTS', 2)
    monkeypatch.setattr(source, 'wait_for', lambda _when: False)
    monkeypatch.setattr(
        source, '_retrieve',
        lambda _t: attempts.append(1) or RetrievalFailure(
            f'HTTP {status}', status_code=status,
        ),
    )

    assert source._retrieve_with_retries(source.start_time) is None
    assert len(attempts) == 2


@pytest.mark.parametrize('status', [400, 401, 403])
def test_authentication_and_invalid_requests_fail_without_retry(config, monkeypatch, status):
    source = _source(
        config,
        datetime(2025, 4, 1, 12, tzinfo=UTC),
        datetime(2025, 4, 1, 13, tzinfo=UTC),
    )
    attempts = []
    monkeypatch.setattr(
        source, '_retrieve',
        lambda _t: attempts.append(1) or RetrievalFailure(f'HTTP {status}', status_code=status),
    )

    assert source._retrieve_with_retries(source.start_time) is None
    assert len(attempts) == 1
    error = source.queue.get_nowait()
    assert isinstance(error, SourceErrorCommand)
    assert error.stop


def test_retry_after_supports_seconds_and_http_dates():
    now = datetime(2025, 4, 1, 12, tzinfo=UTC)

    assert _retry_after('120', now) == timedelta(minutes=2)
    assert _retry_after('Tue, 01 Apr 2025 12:03:00 GMT', now) == timedelta(minutes=3)
    assert _retry_after('Tue, 01 Apr 2025 11:00:00 GMT', now) == timedelta(0)
    assert _retry_after('not a date', now) is None


def test_historical_retrieval_failure_never_emits_done(config, monkeypatch):
    source = _source(
        config,
        datetime(2025, 4, 1, 12, tzinfo=UTC),
        datetime(2025, 4, 1, 14, tzinfo=UTC),
    )
    monkeypatch.setattr(source, '_retrieve_with_retries', lambda _t: None)

    source.run_historical()

    assert source.queue.empty()
