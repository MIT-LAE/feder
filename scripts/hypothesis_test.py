#!/usr/bin/env python3
"""Property-based equivalence test: random FlightQuery calls must return
identical results from the bz2 and lz4 stores.

Required environment variables:

    BZ2_CODE_DIR  BZ2_DATA_DIR  LZ4_CODE_DIR  LZ4_DATA_DIR

Usage:
    BZ2_CODE_DIR=... BZ2_DATA_DIR=... LZ4_CODE_DIR=... LZ4_DATA_DIR=... \\
        pytest scripts/hypothesis_test.py -v --hypothesis-seed=0

A single harness (two executor subprocesses) is started once per pytest
session and reused across all hypothesis examples.
"""

from __future__ import annotations

import math
import os
from datetime import datetime, timedelta, UTC

import pytest
from hypothesis import given, settings, HealthCheck
from hypothesis import strategies as st

from _query_harness import ComparisonHarness


_REQUIRED = ('BZ2_CODE_DIR', 'BZ2_DATA_DIR', 'LZ4_CODE_DIR', 'LZ4_DATA_DIR')
_missing = [k for k in _REQUIRED if not os.environ.get(k)]
if _missing:
    pytest.skip(
        f'missing required env vars: {", ".join(_missing)}',
        allow_module_level=True,
    )


@pytest.fixture(scope='session')
def harness():
    with ComparisonHarness.from_env() as h:
        yield h


_EPOCH = datetime(2025, 1, 1, tzinfo=UTC)
_DATA_DAYS = 365

# Bounding boxes model the primary use case: regions around ground-based
# cameras covering their effective field of view.  Width and height are
# each drawn independently from [10 km, 200 km].
_KM_PER_DEG_LAT = 111.0
_MIN_BOX_KM = 10.0
_MAX_BOX_KM = 200.0


@st.composite
def time_range_strategy(draw):
    start_offset_days = draw(st.integers(min_value=0, max_value=_DATA_DAYS - 1))
    start_offset_hours = draw(st.integers(min_value=0, max_value=23))
    start_offset_minutes = draw(st.integers(min_value=0, max_value=59))
    duration_minutes = draw(st.integers(min_value=5, max_value=30))
    t1 = _EPOCH + timedelta(
        days=start_offset_days,
        hours=start_offset_hours,
        minutes=start_offset_minutes,
    )
    t2 = t1 + timedelta(minutes=duration_minutes)
    return t1, t2


@st.composite
def bbox_strategy(draw):
    center_lat = draw(st.floats(min_value=25.0, max_value=48.0))
    center_lon = draw(st.floats(min_value=-125.0, max_value=-70.0))
    ns_km = draw(st.floats(min_value=_MIN_BOX_KM, max_value=_MAX_BOX_KM))
    ew_km = draw(st.floats(min_value=_MIN_BOX_KM, max_value=_MAX_BOX_KM))
    lat_half = (ns_km / 2.0) / _KM_PER_DEG_LAT
    lon_half = (ew_km / 2.0) / (_KM_PER_DEG_LAT * math.cos(math.radians(center_lat)))
    return {
        'min_lat': center_lat - lat_half,
        'max_lat': center_lat + lat_half,
        'min_lon': center_lon - lon_half,
        'max_lon': center_lon + lon_half,
    }


temporal_strategy = st.sampled_from([
    'time_intersects', 'time_within', 'time_starts_in', 'time_ends_in',
])


def _build_ops(temporal: str, bbox: dict, filter_wp: bool) -> list:
    ops: list = [
        [temporal, []],
        ['spatially_crosses', []],
        ['with_bounds', [bbox]],
    ]
    if filter_wp:
        ops.append(['filter_waypoints', []])
    return ops


@given(
    time_range=time_range_strategy(),
    bbox=bbox_strategy(),
    temporal=temporal_strategy,
    filter_wp=st.booleans(),
)
@settings(
    max_examples=2000,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
    deadline=None,
)
def test_bz2_lz4_equivalence(harness, time_range, bbox, temporal, filter_wp):
    t1, t2 = time_range
    ops = _build_ops(temporal, bbox, filter_wp)
    print(f't1={t1} t2={t2} temporal={temporal} bbox={bbox} filter_wp={filter_wp}')
    verdict = harness.compare(t1, t2, ops)
    assert verdict.ok, f'{temporal} bbox={bbox} filter_wp={filter_wp}: {verdict.message}'
