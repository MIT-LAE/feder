#!/usr/bin/env python3
"""Long-running subprocess that executes FlightQuery requests against a
single feder data directory and returns digests or full point bytes over
stdio.

Invoked by scripts/_query_harness.py as:

    uv --project <code_dir> run python scripts/query_executor.py \\
        <data_dir> --label {bz2,lz4}

Protocol (JSON, newline-delimited on stdout):

    ready  : {"ready": true, "feder_version": "...", "label": "bz2"}
    query  : {"t1": iso, "t2": iso,
              "ops": [["time_starts_in", []], ["with_bounds", [{...}]], ...],
              "full": false}
    digest : {"ok": true, "trajectories": [
                  {"source_id": "...", "n_points": 42,
                   "points_sha256": "<hex>"}, ...]}
    full   : {"ok": true, "trajectories": [
                  {"source_id": "...", "n_points": 42,
                   "points_b64": "<base64>"}, ...]}
    error  : {"ok": false, "error": "<type>: <msg>"}

The process loops until stdin EOF.  All per-query exceptions are caught
and reported as error replies; only import-time failures or OS signals
take the process down.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import sys
import traceback
from datetime import datetime


_ALLOWED_OPS = frozenset({
    'time_intersects', 'time_within', 'time_starts_in', 'time_ends_in',
    'spatially_crosses', 'with_bounds', 'filter_waypoints', 'with_orig',
})


def _emit(obj: dict) -> None:
    sys.stdout.write(json.dumps(obj))
    sys.stdout.write('\n')
    sys.stdout.flush()


def _build_query(FlightQuery, req: dict):
    t1 = datetime.fromisoformat(req['t1'])
    t2 = datetime.fromisoformat(req['t2'])
    q = FlightQuery(t1, t2)
    for op in req.get('ops', []):
        name, args = op[0], op[1]
        if name not in _ALLOWED_OPS:
            raise ValueError(f'disallowed op: {name!r}')
        method = getattr(q, name)
        if args and isinstance(args[0], dict):
            q = method(**args[0])
        else:
            q = method(*args)
    return q


def _digest_reply(Point, trajectories: list) -> dict:
    out = []
    for t in trajectories:
        blob = Point.pack(t.points)
        out.append({
            'source_id': t.source_id,
            'n_points': len(t.points),
            'points_sha256': hashlib.sha256(blob).hexdigest(),
        })
    return {'ok': True, 'trajectories': out}


def _full_reply(Point, trajectories: list) -> dict:
    out = []
    for t in trajectories:
        blob = Point.pack(t.points)
        out.append({
            'source_id': t.source_id,
            'n_points': len(t.points),
            'points_b64': base64.b64encode(blob).decode('ascii'),
        })
    return {'ok': True, 'trajectories': out}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('data_dir', help='feder data directory')
    parser.add_argument('--label', required=True, choices=['bz2', 'lz4'],
                        help='label echoed in the handshake')
    args = parser.parse_args()

    os.environ['FEDER_DATA_DIR'] = args.data_dir

    import feder
    from feder import FlightQuery
    from feder_common.models import Point

    _emit({
        'ready': True,
        'feder_version': getattr(feder, '__version__', 'unknown'),
        'label': args.label,
        'data_dir': args.data_dir,
    })

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
            q = _build_query(FlightQuery, req)
            trajectories = sorted(q.run(), key=lambda t: t.source_id)
            if req.get('full'):
                _emit(_full_reply(Point, trajectories))
            else:
                _emit(_digest_reply(Point, trajectories))
        except Exception as e:
            print(traceback.format_exc(), file=sys.stderr, flush=True)
            _emit({'ok': False, 'error': f'{type(e).__name__}: {e}'})


if __name__ == '__main__':
    main()
