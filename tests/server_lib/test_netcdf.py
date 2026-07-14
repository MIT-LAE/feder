from datetime import datetime, timezone

import netCDF4
import pytest

from feder_common import DataSource, Point
import feder_common.models as common_models
from feder_server import Trajectory, TrajectoryBatch
from feder_server.netcdf import (
    CF_CONVENTIONS,
    FEDER_NETCDF_FILE_TYPE,
    FEDER_NETCDF_FILE_VERSION,
    NetCDFTrajectoryBatchError,
    read_trajectory_batch_netcdf,
    write_trajectory_batch_netcdf,
)


def test_trajectory_batch_netcdf_semantic_round_trip(tmp_path):
    batch = TrajectoryBatch(
        trajectories=[
            Trajectory(common_models.Trajectory(
                source_id="fa-1",
                source=DataSource.FLIGHTAWARE,
                transponder_id="ABC123",
                orig=None,
                dest="KBOS",
                callsign="CALL1",
                aircraft_type=None,
                points=[
                    Point(
                        time=datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),
                        lon=-71.0,
                        lat=42.0,
                        alt=None,
                        alt_gnss=30000.0,
                        heading=None,
                        on_ground=False,
                    ),
                    Point(
                        time=datetime(2024, 1, 1, 12, 1, tzinfo=timezone.utc),
                        lon=-70.5,
                        lat=42.5,
                        alt=31000.0,
                        alt_gnss=None,
                        heading=90.0,
                        on_ground=False,
                    ),
                ],
                partial=True,
            )),
            Trajectory(common_models.Trajectory(
                source_id="ca-2",
                source=DataSource.CONTRAILS_API,
                transponder_id="DEF456",
                orig="KJFK",
                dest=None,
                callsign="CALL2",
                aircraft_type="B738",
                points=[
                    Point(
                        time=datetime(2024, 1, 1, 13, 0, tzinfo=timezone.utc),
                        lon=-72.0,
                        lat=43.0,
                        alt=28000.0,
                        alt_gnss=28100.0,
                        heading=180.0,
                        on_ground=True,
                    ),
                ],
            )),
        ],
        source="receiver-a",
        trajectory_count=42,
    )
    path = tmp_path / "batch.nc"

    write_trajectory_batch_netcdf(path, batch, metadata={"debug_id": "test-run"})
    decoded = read_trajectory_batch_netcdf(path)

    assert decoded.source == batch.source
    assert decoded.trajectory_count == batch.trajectory_count
    assert decoded.trajectories[0].model.partial is False
    assert decoded.trajectories[0].model.orig is None
    assert decoded.trajectories[0].model.dest == "KBOS"
    assert decoded.trajectories[0].model.aircraft_type is None
    assert decoded.trajectories[0].model.points[0].alt is None
    assert decoded.trajectories[0].model.points[0].heading is None
    assert decoded.trajectories[0].model.points[1].alt_gnss is None
    assert decoded.trajectories[1].model.dest is None
    assert decoded.trajectories[1].model == common_models.Trajectory(
        source_id="ca-2",
        source=DataSource.CONTRAILS_API,
        transponder_id="DEF456",
        orig="KJFK",
        dest=None,
        callsign="CALL2",
        aircraft_type="B738",
        points=batch.trajectories[1].model.points,
        partial=False,
    )

    with netCDF4.Dataset(path) as ds:
        assert ds.Conventions == CF_CONVENTIONS
        assert ds.featureType == "trajectory"
        assert ds.feder_file_type == FEDER_NETCDF_FILE_TYPE
        assert ds.feder_file_version == FEDER_NETCDF_FILE_VERSION
        assert ds.feder_source == "receiver-a"
        assert ds.feder_debug_id == "test-run"
        assert set(ds.dimensions) >= {"trajectory", "obs"}
        assert ds.variables["row_size"].sample_dimension == "obs"
        assert ds.variables["row_size"][:].tolist() == [2, 1]
        assert ds.variables["trajectory_id"].cf_role == "trajectory_id"
        assert ds.variables["alt"][:].mask.tolist() == [True, False, False]
        assert ds.variables["alt_gnss"][:].mask.tolist() == [False, True, False]
        assert ds.variables["heading"][:].mask.tolist() == [True, False, False]


def test_netcdf_rejects_end_of_day_marker(tmp_path):
    with pytest.raises(NetCDFTrajectoryBatchError, match="end-of-day"):
        write_trajectory_batch_netcdf(
            tmp_path / "eod.nc",
            TrajectoryBatch(trajectories=[], source="receiver-a", trajectory_count=1),
        )


def test_netcdf_rejects_unsupported_version(tmp_path):
    path = tmp_path / "bad-version.nc"
    _write_minimal_valid_file(path)
    with netCDF4.Dataset(path, "a") as ds:
        ds.feder_file_version = "99.0"

    with pytest.raises(NetCDFTrajectoryBatchError, match="unsupported.*version"):
        read_trajectory_batch_netcdf(path)


def test_netcdf_rejects_invalid_schema(tmp_path):
    path = tmp_path / "bad-schema.nc"
    _write_minimal_valid_file(path)
    with netCDF4.Dataset(path, "a") as ds:
        ds.variables["row_size"].sample_dimension = "not_obs"

    with pytest.raises(NetCDFTrajectoryBatchError, match="row_size must declare"):
        read_trajectory_batch_netcdf(path)


def _write_minimal_valid_file(path):
    batch = TrajectoryBatch(
        trajectories=[
            Trajectory(common_models.Trajectory(
                source_id="id",
                source=DataSource.FLIGHTAWARE,
                transponder_id="ABC",
                orig=None,
                dest=None,
                callsign="CALL",
                aircraft_type=None,
                points=[Point(
                    time=datetime(2024, 1, 1, tzinfo=timezone.utc),
                    lon=1.0,
                    lat=2.0,
                    alt=None,
                    alt_gnss=None,
                    heading=None,
                    on_ground=False,
                )],
            ))
        ],
        source="receiver-a",
        trajectory_count=1,
    )
    write_trajectory_batch_netcdf(path, batch)
