#!/usr/bin/env python3
"""Property-based equivalence test: random FlightQuery calls must return
identical results from the bz2 and lz4 directories.

Run with pytest on the cluster after verify_sample.py and verify_queries.py
pass.  Requires both data directories to be set via environment variables.

Usage:
    BZ2_DIR=/data2/feder/main LZ4_DIR=/data2/feder/temp_lz4 \
        pytest scripts/hypothesis_test.py -v --hypothesis-seed=0

Environment variables:
    BZ2_DIR   path to the original bz2 data directory
    LZ4_DIR   path to the converted lz4 data directory
"""

import os
from datetime import datetime, timedelta, UTC, date

import pytest
from hypothesis import given, settings, HealthCheck
from hypothesis import strategies as st

BZ2_DIR = os.environ.get('BZ2_DIR')
LZ4_DIR = os.environ.get('LZ4_DIR')

if not BZ2_DIR or not LZ4_DIR:
    pytest.skip(
        'BZ2_DIR and LZ4_DIR environment variables must be set',
        allow_module_level=True
    )


# ── query parameter strategies ───────────────────────────────────────────────

# Anchor times to dates known to have data.  Adjust these bounds to match
# the actual date range present in your converted lz4 directory.
_EPOCH = datetime(2025, 1, 1, tzinfo=UTC)
_DATA_DAYS = 365  # adjust to actual span of converted data

@st.composite
def time_range_strategy(draw):
    start_offset_days = draw(st.integers(min_value=0, max_value=_DATA_DAYS - 1))
    start_offset_hours = draw(st.integers(min_value=0, max_value=23))
    duration_minutes = draw(st.integers(min_value=15, max_value=240))
    t1 = _EPOCH + timedelta(days=start_offset_days, hours=start_offset_hours)
    t2 = t1 + timedelta(minutes=duration_minutes)
    return t1, t2


bbox_strategy = st.one_of(
    st.just({}),  # no spatial bounds
    st.fixed_dictionaries({
        'min_lat': st.floats(min_value=20.0, max_value=48.0),
        'max_lat': st.floats(min_value=24.0, max_value=52.0),
        'min_lon': st.floats(min_value=-130.0, max_value=-70.0),
        'max_lon': st.floats(min_value=-126.0, max_value=-66.0),
    }).filter(lambda b: b['min_lat'] < b['max_lat'] and b['min_lon'] < b['max_lon'])
)

temporal_type_strategy = st.sampled_from([
    'intersects', 'within', 'starts_in', 'ends_in'
])

filter_waypoints_strategy = st.booleans()


# ── helpers ───────────────────────────────────────────────────────────────────

def build_and_run(data_dir: str, t1, t2, bbox: dict,
                  temporal_type: str, filter_wp: bool) -> list:
    os.environ['FEDER_DATA_DIR'] = data_dir
    from feder import FlightQuery

    q = FlightQuery(t1, t2)
    match temporal_type:
        case 'intersects': q = q.time_intersects()
        case 'within':     q = q.time_within()
        case 'starts_in':  q = q.time_starts_in()
        case 'ends_in':    q = q.time_ends_in()

    if bbox:
        # Only add spatial query if we have bounds; choose CROSSES.
        q = q.spatially_crosses().with_bounds(**bbox)

    if filter_wp and bbox:
        q = q.filter_waypoints()

    return sorted(list(q.run()), key=lambda t: t.source_id)


def assert_results_equal(bz2_results: list, lz4_results: list) -> None:
    assert len(bz2_results) == len(lz4_results), (
        f'result count differs: bz2={len(bz2_results)} lz4={len(lz4_results)}'
    )
    for ta, tb in zip(bz2_results, lz4_results):
        assert ta.source_id == tb.source_id
        assert len(ta.points) == len(tb.points), (
            f'trajectory {ta.source_id}: '
            f'point count {len(ta.points)} != {len(tb.points)}'
        )
        for pa, pb in zip(ta.points, tb.points):
            assert pa == pb, (
                f'trajectory {ta.source_id}: point mismatch {pa} != {pb}'
            )


# ── test ──────────────────────────────────────────────────────────────────────

@given(
    time_range=time_range_strategy(),
    bbox=bbox_strategy,
    temporal_type=temporal_type_strategy,
    filter_wp=filter_waypoints_strategy,
)
@settings(
    max_examples=2000,
    suppress_health_check=[HealthCheck.too_slow],
    deadline=None,
)
def test_bz2_lz4_equivalence(time_range, bbox, temporal_type, filter_wp):
    t1, t2 = time_range
    bz2_results = build_and_run(BZ2_DIR, t1, t2, bbox, temporal_type, filter_wp)
    lz4_results = build_and_run(LZ4_DIR, t1, t2, bbox, temporal_type, filter_wp)
    assert_results_equal(bz2_results, lz4_results)
