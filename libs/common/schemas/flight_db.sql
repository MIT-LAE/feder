CREATE VIRTUAL TABLE IF NOT EXISTS trajectory_index USING rtree(
  id,
  min_timestamp, max_timestamp,
  min_latitude, max_latitude,
  min_longitude, max_longitude,
  min_altitude, max_altitude
);

CREATE TABLE IF NOT EXISTS trajectories (
  id PRIMARY KEY AUTOINCREMENT,
  source TEXT NOT NULL,
  source_id TEXT NOT NULL,
  transponder_id TEXT NOT NULL,
  callsign TEXT NOT NULL,
  aircraft_type TEXT,
  points BLOB NOT NULL /* Protocol Buffers Points message (points.proto) */
);
