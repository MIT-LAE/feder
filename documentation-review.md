# Documentation review

Reviewed the pdoc-generated documentation under `docs/`, the documentation source/docstrings in `api/src/feder`, and the public API exported by `api/src/feder/__init__.py` plus the vendored `feder.common` code from `libs/common/src/feder_common`.

## Summary

The generated API reference includes the current public API exported by `feder.__all__`:

- query helpers: `FlightQuery`, `get_flights`
- availability helpers: `available_days`, `available_times`, `available_sources`
- models/enums: `DataSource`, `Point`, `Trajectory`, `BoundingBox`, `TemporalQueryType`, `SpatialQueryType`
- version helper: `get_feder_version`
- tutorial submodule

The documentation gaps found in this review have been fixed in the source docstrings/tutorial and the pdoc HTML has been regenerated with `make docs`.

## Issues fixed

1. Corrected `FlightQuery` documentation: spatial filtering is disabled by default, and queries with bounds must explicitly call `spatially_crosses()` or `spatially_within()`.
2. Updated the quickstart example and prose to use timezone-aware UTC datetimes and to describe the actual query shown.
3. Replaced the stale `doc/api-reference.md#Trajectory` link with the generated pdoc `#Trajectory` anchor.
4. Aligned the tutorial's Python version guidance with `api/pyproject.toml` (`>=3.12`).
5. Fixed tutorial examples by importing `UTC` and removing the undeclared `np` dependency.
6. Documented that query times should be timezone-aware UTC datetimes.
7. Fixed swapped `Trajectory.source` / `Trajectory.source_id` field descriptions.
8. Corrected `available_sources` prose to describe its return value as a `set[DataSource]`.
9. Documented `*` wildcard behavior for callsign, origin, and destination filters.
10. Documented that `filter_waypoints()` returns trajectories with `partial=True`.
11. Fixed the `with_bounds()` "latidude" typo.
12. Updated the pdoc logo URL from the Athena-specific `/pages/iross/...` path to the public GitHub Pages URL.
13. Expanded availability-helper docstrings to mention `FEDER_DATA_DIR`.

## Regeneration

Ran:

```shell
make docs
```

This regenerated:

- `docs/feder.html`
- `docs/feder/tutorial.html`
- `docs/search.js`
