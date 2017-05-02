#!/bin/bash
set -e

psql `cat ~/.sqlconninfo` <<EOF
CREATE TABLE tmp_points2 AS
SELECT
  trip_id,
  ST_SetSRID(ST_MakePoint(pickup_longitude, pickup_latitude), 4326) as pickup,
  ST_SetSRID(ST_MakePoint(dropoff_longitude, dropoff_latitude), 4326) as dropoff
FROM taxi_ingest_col
WHERE (abs(pickup_longitude + 73.95) < 1.0 AND
       abs(pickup_latitude - 40.75) < 1.0 ) OR 
      (abs(dropoff_longitude + 73.95) < 1.0 AND
       abs(dropoff_latitude - 40.75) < 1.0 
      );
EOF

psql `cat ~/.sqlconninfo` -c "CREATE INDEX idx_tmp_points_start2 ON tmp_points2 USING gist (pickup);" &
psql `cat ~/.sqlconninfo` -c "CREATE INDEX idx_tmp_points_end2 ON tmp_points2 USING gist (dropoff);" &

wait 

psql `cat ~/.sqlconninfo` <<EOF &
CREATE TABLE tmp_starts_ct2 AS
SELECT t.trip_id, n.gid as pickup_ct_id
FROM tmp_points2 t, nyct2010 n
WHERE ST_Within(t.pickup, n.geom);
CREATE INDEX on tmp_starts_ct2(trip_id);
EOF

psql `cat ~/.sqlconninfo` <<EOF &
CREATE TABLE tmp_starts_tz2 AS
SELECT t.trip_id, n.gid as pickup_location_id
FROM tmp_points2 t, taxi_zones n
WHERE ST_Within(t.pickup, n.geom);
CREATE INDEX on tmp_starts_tz2(trip_id);
EOF

psql `cat ~/.sqlconninfo` <<EOF &
CREATE TABLE tmp_ends_ct2 AS
SELECT t.trip_id, n.gid as dropoff_ct_id
FROM tmp_points2 t, nyct2010 n
WHERE ST_Within(t.dropoff, n.geom);
CREATE INDEX on tmp_ends_ct2(trip_id);
EOF

psql `cat ~/.sqlconninfo` <<EOF &
CREATE TABLE tmp_ends_tz2 AS
SELECT t.trip_id, n.gid as dropoff_location_id
FROM tmp_points2 t, taxi_zones n
WHERE ST_Within(t.dropoff, n.geom);
CREATE INDEX on tmp_ends_tz2(trip_id);
EOF

wait

#psql `cat ~/.sqlconninfo` <<EOF
#  ALTER TABLE taxi_ingest ADD COLUMN IF NOT EXISTS pickup_ct_id INTEGER;
#  ALTER TABLE taxi_ingest ADD COLUMN IF NOT EXISTS dropoff_ct_id INTEGER;
#UPDATE taxi_ingest 
#SET
#    pickup_ct_id = n.pickup_ct_id,
#    dropoff_ct_id = n.dropoff_ct_id,
#    pickup_location_id = n.pickup_location_id,
#    dropoff_location_id = n.dropoff_location_id
#FROM (SELECT * from tmp_starts_ct2 
#FULL OUTER JOIN tmp_starts_tz2 USING (trip_id)
#FULL OUTER JOIN tmp_ends_ct2 USING (trip_id)
#FULL OUTER JOIN tmp_ends_tz2 USING (trip_id)) as n
#WHERE n.trip_id = taxi_ingest.trip_id;
#EOF
#
#psql `cat ~/.sqlconninfo` <<EOF
#DROP INDEX idx_tmp_points_start2;
#DROP INDEX idx_tmp_points_end2;
#DROP TABLE tmp_points2 CASCADE;
#DROP TABLE tmp_starts_ct2 CASCADE;
#DROP TABLE tmp_starts_tz2 CASCADE;
#DROP TABLE tmp_ends_ct2 CASCADE;
#DROP TABLE tmp_ends_tz2 CASCADE;
#EOF
