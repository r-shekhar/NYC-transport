#!/bin/bash

psql `cat ~/.sqlconninfo` <<EOF
CREATE TABLE tmp_points AS
SELECT
  biketrip_id,
  ST_SetSRID(ST_MakePoint(start_station_longitude, start_station_latitude), 4326) as start_station,
  ST_SetSRID(ST_MakePoint(end_station_longitude, end_station_latitude), 4326) as end_station
FROM bike_ingest
WHERE (abs(start_station_longitude + 73.95) < 1.0 AND
       abs(start_station_latitude - 40.75) < 1.0 ) OR 
      (abs(end_station_longitude + 73.95) < 1.0 AND
       abs(end_station_latitude - 40.75) < 1.0 
      );
EOF
      

psql `cat ~/.sqlconninfo` -c "CREATE INDEX idx_tmp_points_start ON tmp_points USING gist (start_station);" &
psql `cat ~/.sqlconninfo` -c "CREATE INDEX idx_tmp_points_end ON tmp_points USING gist (end_station);" &

wait 

psql `cat ~/.sqlconninfo` <<EOF
CREATE TABLE tmp_starts_ct AS
SELECT t.biketrip_id, n.gid as start_ct_id
FROM tmp_points t, nyct2010 n
WHERE ST_Within(t.start_station, n.geom);
CREATE INDEX on tmp_starts_ct(biketrip_id);
EOF &

psql `cat ~/.sqlconninfo` <<EOF
CREATE TABLE tmp_starts_tz AS
SELECT t.biketrip_id, n.gid as start_taxizone_id
FROM tmp_points t, taxi_zones n
WHERE ST_Within(t.start_station, n.geom);
CREATE INDEX on tmp_starts_tz(biketrip_id);
EOF &

psql `cat ~/.sqlconninfo` <<EOF
CREATE TABLE tmp_ends_ct AS
SELECT t.biketrip_id, n.gid as end_ct_id
FROM tmp_points t, nyct2010 n
WHERE ST_Within(t.end_station, n.geom);
CREATE INDEX on tmp_ends_ct(biketrip_id);
EOF &

psql `cat ~/.sqlconninfo` <<EOF
CREATE TABLE tmp_ends_tz AS
SELECT t.biketrip_id, n.gid as end_taxizone_id
FROM tmp_points t, taxi_zones n
WHERE ST_Within(t.end_station, n.geom);
CREATE INDEX on tmp_ends_tz(biketrip_id);
EOF &

wait

psql `cat ~/.sqlconninfo` <<EOF
UPDATE bike_ingest 
SET
    start_ct_id = n.start_ct_id,
    end_ct_id = n.end_ct_id,
    start_taxizone_id = n.start_taxizone_id,
    end_taxizone_id = n.end_taxizone_id
FROM (SELECT * from tmp_starts_ct 
FULL OUTER JOIN tmp_starts_tz USING (biketrip_id)
FULL OUTER JOIN tmp_ends_ct USING (biketrip_id)
FULL OUTER JOIN tmp_ends_tz USING (biketrip_id)) as n
WHERE n.biketrip_id = bike_ingest.biketrip_id
;
DROP INDEX tmp_ends_ct_biketrip_id_idx;
DROP INDEX tmp_ends_tz_biketrip_id_idx;
DROP INDEX tmp_starts_ct_biketrip_id_idx;
DROP INDEX tmp_starts_tz_biketrip_id_idx;
DROP INDEX idx_tmp_points_start;
DROP INDEX idx_tmp_points_end;
DROP TABLE tmp_points CASCADE;
DROP TABLE tmp_starts_ct CASCADE;
DROP TABLE tmp_starts_tz CASCADE;
DROP TABLE tmp_ends_ct CASCADE;
DROP TABLE tmp_ends_tz CASCADE;
EOF


