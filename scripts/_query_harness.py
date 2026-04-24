"""Shared plumbing for the bz2-vs-lz4 comparison scripts.

Spawns two `query_executor.py` subprocesses (one per compression format)
under their respective `uv --project` environments, exchanges JSON
requests and replies over stdio, and compares results.  Consumers
(`verify_queries.py`, `hypothesis_test.py`, `soak_test.py`) generate
queries in their own way and call `ComparisonHarness.compare(...)`.

Configuration is read from environment variables:

    BZ2_CODE_DIR   path to the pre-lz4 feder checkout (uv --project root)
    BZ2_DATA_DIR   path to the bz2 feder data directory
    LZ4_CODE_DIR   path to the current feder checkout (uv --project root)
    LZ4_DATA_DIR   path to the lz4 feder data directory

Per-query usage:

    with ComparisonHarness.from_env() as h:
        verdict = h.compare(t1, t2, ops)
        if not verdict.ok:
            print(verdict.message)
"""

from __future__ import annotations

import base64
import datetime as _dt
import json
import os
import select
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path


_HANDSHAKE_TIMEOUT_S = 30.0
_SHUTDOWN_WAIT_S = 5.0

# Path to query_executor.py, resolved once so absolute paths work regardless
# of which uv --project we pass in.
_EXECUTOR_PATH = (Path(__file__).resolve().parent / 'query_executor.py')

_LOG_DIR = Path(__file__).resolve().parent / 'logs'


@dataclass
class Verdict:
    ok: bool
    message: str


def _iso(d: _dt.datetime) -> str:
    return d.isoformat()


def _require_env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise RuntimeError(f'required env var {name} is not set')
    return v


class _Executor:
    """One subprocess wrapper: spawn, handshake, send, recv, shutdown."""

    def __init__(self, label: str, code_dir: str, data_dir: str):
        self.label = label
        self.code_dir = code_dir
        self.data_dir = data_dir
        self.proc: subprocess.Popen | None = None
        self.log_path: Path | None = None
        self._log_file = None

    def start(self) -> dict:
        _LOG_DIR.mkdir(parents=True, exist_ok=True)
        ts = time.strftime('%Y%m%d-%H%M%S')
        self.log_path = _LOG_DIR / (
            f'query_executor_{self.label}_{os.getpid()}_{ts}.log'
        )
        self._log_file = open(self.log_path, 'wb')

        cmd = [
            'uv', '--project', self.code_dir, 'run', 'python',
            str(_EXECUTOR_PATH), self.data_dir, '--label', self.label,
        ]
        self.proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self._log_file,
            text=True,
            bufsize=1,
        )

        line = self._readline_with_timeout(_HANDSHAKE_TIMEOUT_S)
        if line is None:
            self._die_with_context(
                f'{self.label} executor failed to produce a handshake '
                f'within {_HANDSHAKE_TIMEOUT_S}s'
            )
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            self._die_with_context(
                f'{self.label} executor sent non-JSON handshake: {line!r}'
            )
        if not msg.get('ready'):
            self._die_with_context(
                f'{self.label} executor reported not-ready: {msg}'
            )
        if msg.get('label') != self.label:
            self._die_with_context(
                f'{self.label} executor reports label {msg.get("label")!r}, '
                f'expected {self.label!r}'
            )
        return msg

    def send(self, req: dict) -> dict:
        assert self.proc is not None and self.proc.stdin is not None
        try:
            self.proc.stdin.write(json.dumps(req) + '\n')
            self.proc.stdin.flush()
        except BrokenPipeError:
            self._die_with_context(f'{self.label} executor stdin closed')
        line = self._readline_with_timeout(None)
        if line is None:
            self._die_with_context(
                f'{self.label} executor closed stdout (EOF before reply)'
            )
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            self._die_with_context(
                f'{self.label} executor sent non-JSON reply: {line!r}'
            )

    def _readline_with_timeout(self, timeout: float | None) -> str | None:
        assert self.proc is not None and self.proc.stdout is not None
        fd = self.proc.stdout.fileno()
        if timeout is not None:
            rlist, _, _ = select.select([fd], [], [], timeout)
            if not rlist:
                return None
        line = self.proc.stdout.readline()
        if not line:
            return None
        return line.strip()

    def shutdown(self) -> None:
        if self.proc is None:
            return
        try:
            if self.proc.stdin and not self.proc.stdin.closed:
                self.proc.stdin.close()
        except BrokenPipeError:
            pass
        try:
            self.proc.wait(timeout=_SHUTDOWN_WAIT_S)
        except subprocess.TimeoutExpired:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=_SHUTDOWN_WAIT_S)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        if self._log_file:
            self._log_file.close()
            self._log_file = None

    def _die_with_context(self, msg: str) -> None:
        tail = ''
        if self._log_file:
            self._log_file.flush()
        if self.log_path and self.log_path.exists():
            with open(self.log_path, 'r', errors='replace') as f:
                lines = f.readlines()[-50:]
                tail = ''.join(lines)
        full = msg
        if tail:
            full += f'\n--- {self.label} stderr tail ---\n{tail}'
        raise RuntimeError(full)


class ComparisonHarness:
    """Context manager driving two executors and comparing their replies."""

    def __init__(
        self,
        bz2_code: str, bz2_data: str,
        lz4_code: str, lz4_data: str,
    ):
        self.bz2 = _Executor('bz2', bz2_code, bz2_data)
        self.lz4 = _Executor('lz4', lz4_code, lz4_data)

    @classmethod
    def from_env(cls) -> 'ComparisonHarness':
        return cls(
            bz2_code=_require_env('BZ2_CODE_DIR'),
            bz2_data=_require_env('BZ2_DATA_DIR'),
            lz4_code=_require_env('LZ4_CODE_DIR'),
            lz4_data=_require_env('LZ4_DATA_DIR'),
        )

    def __enter__(self) -> 'ComparisonHarness':
        bz2_info = self.bz2.start()
        try:
            lz4_info = self.lz4.start()
        except Exception:
            self.bz2.shutdown()
            raise
        print(
            f'bz2 executor ready (feder {bz2_info.get("feder_version")}, '
            f'data {self.bz2.data_dir}, log {self.bz2.log_path})',
            file=sys.stderr,
        )
        print(
            f'lz4 executor ready (feder {lz4_info.get("feder_version")}, '
            f'data {self.lz4.data_dir}, log {self.lz4.log_path})',
            file=sys.stderr,
        )
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.bz2.shutdown()
        self.lz4.shutdown()

    def compare(
        self,
        t1: _dt.datetime, t2: _dt.datetime,
        ops: list,
    ) -> Verdict:
        req = {'t1': _iso(t1), 't2': _iso(t2), 'ops': ops, 'full': False}
        bz2_reply = self.bz2.send(req)
        lz4_reply = self.lz4.send(req)

        bz2_ok = bz2_reply.get('ok', False)
        lz4_ok = lz4_reply.get('ok', False)

        if not bz2_ok and not lz4_ok:
            if bz2_reply.get('error') == lz4_reply.get('error'):
                return Verdict(True, f'both errored identically: {bz2_reply["error"]}')
            return Verdict(False, (
                f'bz2 error {bz2_reply.get("error")!r} != '
                f'lz4 error {lz4_reply.get("error")!r}'
            ))
        if bz2_ok != lz4_ok:
            return Verdict(False, (
                f'one side errored: bz2_ok={bz2_ok} lz4_ok={lz4_ok} '
                f'bz2={bz2_reply.get("error", "<ok>")!r} '
                f'lz4={lz4_reply.get("error", "<ok>")!r}'
            ))

        bz2_digests = bz2_reply['trajectories']
        lz4_digests = lz4_reply['trajectories']
        if _digests_equal(bz2_digests, lz4_digests):
            return Verdict(True, f'{len(bz2_digests)} trajectories, digests match')

        return self._full_fidelity_verdict(req, bz2_digests, lz4_digests)

    def _full_fidelity_verdict(
        self, req: dict,
        bz2_digests: list, lz4_digests: list,
    ) -> Verdict:
        full_req = {**req, 'full': True}
        bz2_full = self.bz2.send(full_req)
        lz4_full = self.lz4.send(full_req)
        if not (bz2_full.get('ok') and lz4_full.get('ok')):
            return Verdict(False, (
                'digest mismatch, then full rerun errored: '
                f'bz2={bz2_full} lz4={lz4_full}'
            ))
        return Verdict(False, _diff_full(bz2_full['trajectories'],
                                         lz4_full['trajectories']))


def _digests_equal(a: list, b: list) -> bool:
    if len(a) != len(b):
        return False
    for ta, tb in zip(a, b):
        if (ta['source_id'] != tb['source_id']
                or ta['n_points'] != tb['n_points']
                or ta['points_sha256'] != tb['points_sha256']):
            return False
    return True


def _diff_full(a: list, b: list) -> str:
    if len(a) != len(b):
        return f'result count differs: bz2={len(a)} lz4={len(b)}'
    for ta, tb in zip(a, b):
        if ta['source_id'] != tb['source_id']:
            return (f'source_id mismatch at same index: '
                    f'bz2={ta["source_id"]!r} lz4={tb["source_id"]!r}')
        if ta['n_points'] != tb['n_points']:
            return (f'trajectory {ta["source_id"]}: '
                    f'point count bz2={ta["n_points"]} lz4={tb["n_points"]}')
        if ta['points_b64'] == tb['points_b64']:
            continue
        pa = base64.b64decode(ta['points_b64'])
        pb = base64.b64decode(tb['points_b64'])
        # Skip 2-byte length prefix from Point.pack so offset maps to points.
        body_a, body_b = pa[2:], pb[2:]
        point_size = len(body_a) // ta['n_points'] if ta['n_points'] else 0
        for j in range(ta['n_points']):
            start, end = j * point_size, (j + 1) * point_size
            if body_a[start:end] != body_b[start:end]:
                return (f'trajectory {ta["source_id"]} point {j}: '
                        f'bz2={body_a[start:end].hex()} '
                        f'lz4={body_b[start:end].hex()}')
        return f'trajectory {ta["source_id"]}: points bytes differ (no per-point diff found)'
    return 'digests differed but full-fidelity compare shows no difference'
