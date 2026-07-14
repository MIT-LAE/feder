"""NetCDF trajectory-batch interchange helpers.

This module stores RabbitMQ-equivalent :class:`TrajectoryBatch` payloads as
CF discrete sampling geometry contiguous ragged trajectory arrays.  End-of-day
markers are intentionally not representable: callers must not pass empty
trajectory batches with ordinal counts here.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import netCDF4
import numpy as np

from feder_common import DataSource, Point
import feder_common.models as common_models

from .messages import Trajectory, TrajectoryBatch


FEDER_NETCDF_FILE_TYPE = "feder.trajectory_batch"
FEDER_NETCDF_FILE_VERSION = "1.0"
CF_CONVENTIONS = "CF-1.8"

_TRAJ_DIM = "trajectory"
_OBS_DIM = "obs"
_FILL_FLOAT = np.float64(-999999.0)
_FILL_INT = np.int64(-2147483647)
_FILL_BYTE = np.int8(-127)
_NULL_STRING = ""


class NetCDFTrajectoryBatchError(ValueError):
    """Raised when a Feder trajectory-batch NetCDF file is invalid."""


def write_trajectory_batch_netcdf(
        path: str | Path,
        batch: TrajectoryBatch,
        *,
        metadata: Mapping[str, Any] | None = None
) -> None:
    """Write a trajectory batch to a Feder NetCDF interchange file.

    ``batch`` must contain one or more trajectories.  Empty trajectory batches
    are RabbitMQ end-of-day markers in Feder and are intentionally rejected by
    this file protocol.
    """
    if len(batch.trajectories) == 0:
        raise NetCDFTrajectoryBatchError(
            "empty trajectory batches are end-of-day markers and cannot be written"
        )

    models = [traj.model for traj in batch.trajectories]
    row_sizes = np.array([len(traj.points) for traj in models], dtype="i4")
    nobs = int(row_sizes.sum())

    with netCDF4.Dataset(path, "w", format="NETCDF4") as ds:
        ds.createDimension(_TRAJ_DIM, len(models))
        ds.createDimension(_OBS_DIM, nobs)

        ds.Conventions = CF_CONVENTIONS
        ds.featureType = "trajectory"
        ds.feder_file_type = FEDER_NETCDF_FILE_TYPE
        ds.feder_file_version = FEDER_NETCDF_FILE_VERSION
        ds.feder_source = batch.source
        ds.feder_trajectory_count = np.uint32(batch.trajectory_count)
        ds.feder_created_utc = datetime.now(timezone.utc).isoformat()
        if metadata is not None:
            for key, value in metadata.items():
                ds.setncattr(f"feder_{key}", value)

        trajectory_id = ds.createVariable("trajectory_id", str, (_TRAJ_DIM,))
        trajectory_id.cf_role = "trajectory_id"
        trajectory_id.long_name = "Feder source-specific trajectory identifier"

        row_size = ds.createVariable("row_size", "i4", (_TRAJ_DIM,))
        row_size.long_name = "number of observations for this trajectory"
        row_size.sample_dimension = _OBS_DIM

        source = ds.createVariable("source", "i2", (_TRAJ_DIM,))
        source.long_name = "Feder DataSource enum value"

        transponder_id = ds.createVariable("transponder_id", str, (_TRAJ_DIM,))
        callsign = ds.createVariable("callsign", str, (_TRAJ_DIM,))
        orig = ds.createVariable("orig", str, (_TRAJ_DIM,))
        dest = ds.createVariable("dest", str, (_TRAJ_DIM,))
        aircraft_type = ds.createVariable("aircraft_type", str, (_TRAJ_DIM,))
        orig_present = ds.createVariable("orig_present", "i1", (_TRAJ_DIM,))
        dest_present = ds.createVariable("dest_present", "i1", (_TRAJ_DIM,))
        aircraft_type_present = ds.createVariable("aircraft_type_present", "i1", (_TRAJ_DIM,))
        for var in (orig_present, dest_present, aircraft_type_present):
            var.flag_values = np.array([0, 1], dtype="i1")
            var.flag_meanings = "missing present"

        time = ds.createVariable("time", "i8", (_OBS_DIM,), fill_value=_FILL_INT)
        time.standard_name = "time"
        time.units = "seconds since 1970-01-01 00:00:00 UTC"
        time.calendar = "standard"

        lon = ds.createVariable("lon", "f8", (_OBS_DIM,), fill_value=_FILL_FLOAT)
        lon.standard_name = "longitude"
        lon.units = "degrees_east"
        lat = ds.createVariable("lat", "f8", (_OBS_DIM,), fill_value=_FILL_FLOAT)
        lat.standard_name = "latitude"
        lat.units = "degrees_north"
        alt = ds.createVariable("alt", "f8", (_OBS_DIM,), fill_value=_FILL_FLOAT)
        alt.long_name = "barometric altitude"
        alt.units = "ft"
        alt_gnss = ds.createVariable("alt_gnss", "f8", (_OBS_DIM,), fill_value=_FILL_FLOAT)
        alt_gnss.long_name = "GNSS altitude"
        alt_gnss.units = "ft"
        heading = ds.createVariable("heading", "f8", (_OBS_DIM,), fill_value=_FILL_FLOAT)
        heading.long_name = "heading"
        heading.units = "degrees"
        on_ground = ds.createVariable("on_ground", "i1", (_OBS_DIM,), fill_value=_FILL_BYTE)
        on_ground.flag_values = np.array([0, 1], dtype="i1")
        on_ground.flag_meanings = "false true"

        _assign_strings(trajectory_id, [traj.source_id for traj in models])
        row_size[:] = row_sizes
        source[:] = np.array([traj.source.value for traj in models], dtype="i2")
        _assign_strings(transponder_id, [traj.transponder_id for traj in models])
        _assign_strings(callsign, [traj.callsign for traj in models])
        _assign_strings(orig, [_string_or_null(traj.orig) for traj in models])
        _assign_strings(dest, [_string_or_null(traj.dest) for traj in models])
        _assign_strings(aircraft_type, [_string_or_null(traj.aircraft_type) for traj in models])
        orig_present[:] = np.array([traj.orig is not None for traj in models], dtype="i1")
        dest_present[:] = np.array([traj.dest is not None for traj in models], dtype="i1")
        aircraft_type_present[:] = np.array([traj.aircraft_type is not None for traj in models], dtype="i1")

        points = [point for traj in models for point in traj.points]
        time[:] = np.array([int(point.time.timestamp()) for point in points], dtype="i8")
        lon[:] = np.array([point.lon for point in points], dtype="f8")
        lat[:] = np.array([point.lat for point in points], dtype="f8")
        alt[:] = _optional_float_array(point.alt for point in points)
        alt_gnss[:] = _optional_float_array(point.alt_gnss for point in points)
        heading[:] = _optional_float_array(point.heading for point in points)
        on_ground[:] = np.array([point.on_ground for point in points], dtype="i1")


def read_trajectory_batch_netcdf(path: str | Path) -> TrajectoryBatch:
    """Read and validate a Feder NetCDF trajectory-batch interchange file."""
    with netCDF4.Dataset(path, "r") as ds:
        _validate_dataset(ds)

        row_sizes = np.asarray(ds.variables["row_size"][:], dtype="i4")
        source_values = np.asarray(ds.variables["source"][:], dtype="i2")
        source_ids = _string_values(ds.variables["trajectory_id"][:])
        transponder_ids = _string_values(ds.variables["transponder_id"][:])
        callsigns = _string_values(ds.variables["callsign"][:])
        origs = _nullable_strings(ds.variables["orig"][:], ds.variables["orig_present"][:])
        dests = _nullable_strings(ds.variables["dest"][:], ds.variables["dest_present"][:])
        aircraft_types = _nullable_strings(
            ds.variables["aircraft_type"][:], ds.variables["aircraft_type_present"][:]
        )

        times = np.ma.filled(ds.variables["time"][:], _FILL_INT)
        lons = np.ma.filled(ds.variables["lon"][:], np.nan)
        lats = np.ma.filled(ds.variables["lat"][:], np.nan)
        alts = ds.variables["alt"][:]
        alts_gnss = ds.variables["alt_gnss"][:]
        headings = ds.variables["heading"][:]
        on_grounds = np.ma.filled(ds.variables["on_ground"][:], 0)

        trajectories: list[Trajectory] = []
        offset = 0
        for i, row_count in enumerate(row_sizes):
            points = []
            for j in range(offset, offset + int(row_count)):
                points.append(Point(
                    time=datetime.fromtimestamp(int(times[j]), tz=timezone.utc),
                    lon=float(lons[j]),
                    lat=float(lats[j]),
                    alt=_optional_float(alts[j]),
                    alt_gnss=_optional_float(alts_gnss[j]),
                    heading=_optional_float(headings[j]),
                    on_ground=bool(on_grounds[j]),
                ))
            offset += int(row_count)
            try:
                data_source = DataSource(int(source_values[i]))
            except ValueError as exc:
                raise NetCDFTrajectoryBatchError(
                    f"invalid DataSource value at trajectory {i}: {source_values[i]}"
                ) from exc
            trajectories.append(Trajectory(model=common_models.Trajectory(
                source_id=source_ids[i],
                source=data_source,
                transponder_id=transponder_ids[i],
                orig=origs[i],
                dest=dests[i],
                callsign=callsigns[i],
                aircraft_type=aircraft_types[i],
                points=points,
                partial=False,
            )))

        return TrajectoryBatch(
            trajectories=trajectories,
            source=str(ds.getncattr("feder_source")),
            trajectory_count=int(ds.getncattr("feder_trajectory_count")),
        )


def _validate_dataset(ds: netCDF4.Dataset) -> None:
    if getattr(ds, "Conventions", None) != CF_CONVENTIONS:
        raise NetCDFTrajectoryBatchError(
            f"invalid CF conventions attribute: {getattr(ds, 'Conventions', None)!r}"
        )
    if getattr(ds, "featureType", None) != "trajectory":
        raise NetCDFTrajectoryBatchError(
            f"invalid CF featureType attribute: {getattr(ds, 'featureType', None)!r}"
        )
    if getattr(ds, "feder_file_type", None) != FEDER_NETCDF_FILE_TYPE:
        raise NetCDFTrajectoryBatchError(
            f"invalid Feder file type: {getattr(ds, 'feder_file_type', None)!r}"
        )
    if getattr(ds, "feder_file_version", None) != FEDER_NETCDF_FILE_VERSION:
        raise NetCDFTrajectoryBatchError(
            f"unsupported Feder trajectory-batch file version: "
            f"{getattr(ds, 'feder_file_version', None)!r}"
        )
    for attr_name in ("feder_source", "feder_trajectory_count"):
        if not hasattr(ds, attr_name):
            raise NetCDFTrajectoryBatchError(f"missing required global attribute: {attr_name}")
    for dim_name in (_TRAJ_DIM, _OBS_DIM):
        if dim_name not in ds.dimensions:
            raise NetCDFTrajectoryBatchError(f"missing required dimension: {dim_name}")
    required_vars = [
        "trajectory_id", "row_size", "source", "transponder_id", "callsign",
        "orig", "dest", "aircraft_type", "orig_present", "dest_present",
        "aircraft_type_present", "time", "lon", "lat", "alt", "alt_gnss",
        "heading", "on_ground",
    ]
    for var_name in required_vars:
        if var_name not in ds.variables:
            raise NetCDFTrajectoryBatchError(f"missing required variable: {var_name}")
    row_size = ds.variables["row_size"]
    if getattr(row_size, "sample_dimension", None) != _OBS_DIM:
        raise NetCDFTrajectoryBatchError("row_size must declare sample_dimension='obs'")
    if tuple(row_size.dimensions) != (_TRAJ_DIM,):
        raise NetCDFTrajectoryBatchError("row_size must use the trajectory dimension")
    row_total = int(np.asarray(row_size[:], dtype="i4").sum())
    if row_total != len(ds.dimensions[_OBS_DIM]):
        raise NetCDFTrajectoryBatchError(
            f"row_size sum {row_total} does not match obs dimension {len(ds.dimensions[_OBS_DIM])}"
        )


def _string_or_null(value: str | None) -> str:
    return _NULL_STRING if value is None else value


def _assign_strings(var, values: list[str]) -> None:
    for i, value in enumerate(values):
        var[i] = value


def _optional_float_array(values) -> np.ma.MaskedArray:
    return np.ma.masked_equal(
        np.array([_FILL_FLOAT if value is None else value for value in values], dtype="f8"),
        _FILL_FLOAT,
    )


def _optional_float(value) -> float | None:
    if np.ma.is_masked(value):
        return None
    fval = float(value)
    return None if fval == float(_FILL_FLOAT) else fval


def _string_values(values) -> list[str]:
    return [str(value) for value in values]


def _nullable_strings(values, present_values) -> list[str | None]:
    strings = _string_values(values)
    present = np.asarray(present_values, dtype="i1")
    return [value if bool(flag) else None for value, flag in zip(strings, present)]
