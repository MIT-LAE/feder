from datetime import datetime, timezone
from pathlib import Path
from queue import PriorityQueue
from threading import Thread

from click.testing import CliRunner
import pandas as pd
import pytest

from feder_common import DataSource
from feder_server import read_trajectory_batch_netcdf
import feder_rx
from feder_rx import _validate_file_output_directory
from feder_rx.commands import SourceDoneCommand, SourcePositionCommand
from feder_rx.db import DB
from feder_rx.processor import Processor
from feder_rx.sinks import NetCDFFileTrajectorySink


FILE_ONLY_CONFIG = """
[paths]
data-directory = "{root}/data"
staging-directory = "{root}/staging"
scratch-directory = "{root}/scratch"

[sources]
completion-delay = "15 minutes"
data-lag = 0

[source.contrails-api]
api-key = "dummy"
"""


def _config_file(tmp_path: Path) -> Path:
    for name in ("data", "staging", "scratch"):
        (tmp_path / name).mkdir()
    path = tmp_path / "config.toml"
    path.write_text(FILE_ONLY_CONFIG.format(root=tmp_path), encoding="utf-8")
    return path


def _position(source_id: str = "DUMMY-001") -> SourcePositionCommand:
    return SourcePositionCommand(
        source_id=source_id,
        transponder_id="ABCDEF",
        time=datetime(2025, 4, 1, 12, 0, tzinfo=timezone.utc),
        orig="DUMA",
        dest="DUMZ",
        callsign="DUMMY",
        aircraft_type=None,
        lat=41.0,
        lon=-95.0,
        alt=35000,
        alt_gnss=None,
        heading=None,
        on_ground=False,
    )


def test_file_output_directory_validation(config, tmp_path):
    out = tmp_path / "out"
    assert _validate_file_output_directory(config, str(out)) == out
    assert out.is_dir()

    (out / "rx.00000001.nc").write_text("old", encoding="utf-8")
    with pytest.raises(ValueError, match="visible"):
        _validate_file_output_directory(config, str(out))

    nested = Path(config.data_directory) / "receiver-output"
    with pytest.raises(ValueError, match="nested"):
        _validate_file_output_directory(config, str(nested))


def test_netcdf_sink_atomic_publish_deletes_only_after_success(config, tmp_path, monkeypatch):
    db = DB(config, "file-sink")
    db.save_position(
        source_id="DUMMY-001",
        transponder_id="ABCDEF",
        time=datetime(2025, 4, 1, 12, 0, tzinfo=timezone.utc),
        orig="DUMA",
        dest="DUMZ",
        callsign="DUMMY",
        aircraft_type=None,
        lat=41.0,
        lon=-95.0,
        alt=35000,
        alt_gnss=None,
        heading=None,
        on_ground=False,
    )
    sink = NetCDFFileTrajectorySink(db, tmp_path, "rx-test")
    processor = Processor(
        config,
        DataSource.FLIGHTAWARE,
        "rx-test",
        True,
        db,
        PriorityQueue(),
        source_control=None,
        trajectory_sink=sink,
    )
    source_ids = db.complete_source_ids(datetime(2025, 4, 1, 12, 15, tzinfo=timezone.utc))

    def fail(*_args, **_kwargs):
        raise OSError("boom")

    monkeypatch.setattr("feder_rx.sinks.write_trajectory_batch_netcdf", fail)
    with pytest.raises(OSError):
        processor._send_trajectories(source_ids)
    assert db.count_entries() == 1
    assert list(tmp_path.glob("*.nc")) == []

    monkeypatch.undo()
    processor._send_trajectories(source_ids)
    assert db.count_entries() == 0
    files = sorted(tmp_path.glob("*.nc"))
    assert [p.name for p in files] == ["rx-test.00000002.nc"]
    batch = read_trajectory_batch_netcdf(files[0])
    assert batch.source == "rx-test"
    assert [t.model.source_id for t in batch.trajectories] == ["DUMMY-001"]


def test_file_output_processor_writes_sequence_and_finishes_empty(config, tmp_path):
    db = DB(config, "file-processor", historical=True)
    queue = PriorityQueue()
    sink = NetCDFFileTrajectorySink(db, tmp_path, "rx-file")
    processor = Processor(
        config,
        DataSource.FLIGHTAWARE,
        "rx-file",
        True,
        db,
        queue,
        source_control=None,
        trajectory_sink=sink,
    )

    def feed():
        queue.put(_position("DUMMY-001"))
        queue.put(SourceDoneCommand(datetime(2025, 4, 1, 12, 0, tzinfo=timezone.utc)))

    feeder = Thread(target=feed)
    feeder.start()
    processor.run()
    feeder.join()

    assert db.is_empty()
    assert [p.name for p in sorted(tmp_path.glob("*.nc"))] == ["rx-file.00000001.nc"]


def test_file_output_cli_requires_historical_range_and_rejects_file_sources(tmp_path):
    cfg = _config_file(tmp_path)
    runner = CliRunner()

    result = runner.invoke(feder_rx.run, ["-c", str(cfg), "--file-output-directory", str(tmp_path / "out"), "contrails-api"])
    assert result.exit_code == 1

    result = runner.invoke(
        feder_rx.run,
        [
            "-c", str(cfg),
            "--file-output-directory", str(tmp_path / "out2"),
            "--start-time", "2025-04-01T12:00:00",
            "--end-time", "2025-04-01T12:00:00",
            "csv",
        ],
    )
    assert result.exit_code == 1


def test_file_output_cli_constructs_no_rmq_or_liveness(tmp_path, monkeypatch):
    cfg = _config_file(tmp_path)
    out = tmp_path / "out"

    def explode(*_args, **_kwargs):
        raise AssertionError("RMQ must not be constructed in file-output mode")

    monkeypatch.setattr(feder_rx, "RMQ", explode)
    monkeypatch.setattr(feder_rx, "IngesterLivenessChecker", explode)

    df = pd.DataFrame({
        "flight_id": ["F1"],
        "icao_address": ["ABCDEF"],
        "timestamp": [pd.Timestamp("2025-04-01T12:00:00Z")],
        "callsign": ["DUMMY"],
        "departure_airport_icao": ["DUMA"],
        "arrival_airport_icao": ["DUMZ"],
        "aircraft_type_icao": [None],
        "latitude": [41.0],
        "longitude": [-95.0],
        "altitude_baro": [35000.0],
        "altitude_gnss": [None],
    })
    monkeypatch.setattr(feder_rx.ContrailsAPISource, "_retrieve", lambda self, t: df)

    result = CliRunner().invoke(
        feder_rx.run,
        [
            "-c", str(cfg),
            "--file-output-directory", str(out),
            "--start-time", "2025-04-01T12:00:00",
            "--end-time", "2025-04-01T12:00:00",
            "contrails-api",
        ],
    )

    assert result.exit_code == 0, result.output
    files = sorted(out.glob("*.nc"))
    assert len(files) == 1
    assert files[0].name.startswith("rx-contrails-api-")
    assert files[0].name.endswith(".00000001.nc")
