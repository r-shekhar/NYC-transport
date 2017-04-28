#!/bin/bash
set -e

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
      )
ORDER BY biketrip_id;
EOF
      

psql `cat ~/.sqlconninfo` -c "CREATE INDEX idx_tmp_points_start ON tmp_points USING gist (start_station);" 
psql `cat ~/.sqlconninfo` -c "CREATE INDEX idx_tmp_points_end ON tmp_points USING gist (end_station);" 
wait 

psql `cat ~/.sqlconninfo` <<EOF &
CREATE TABLE tmp_starts_ct AS
SELECT t.biketrip_id, n.gid as start_ct_id
FROM tmp_points t, nyct2010 n
WHERE ST_Within(t.start_station, n.geom);
CREATE INDEX on tmp_starts_ct(biketrip_id);
EOF

psql `cat ~/.sqlconninfo` <<EOF &
CREATE TABLE tmp_starts_tz AS
SELECT t.biketrip_id, n.gid as start_taxizone_id
FROM tmp_points t, taxi_zones n
WHERE ST_Within(t.start_station, n.geom);
CREATE INDEX on tmp_starts_tz(biketrip_id);
EOF

psql `cat ~/.sqlconninfo` <<EOF &
CREATE TABLE tmp_ends_ct AS
SELECT t.biketrip_id, n.gid as end_ct_id
FROM tmp_points t, nyct2010 n
WHERE ST_Within(t.end_station, n.geom);
CREATE INDEX on tmp_ends_ct(biketrip_id);
EOF

psql `cat ~/.sqlconninfo` <<EOF &
CREATE TABLE tmp_ends_tz AS
SELECT t.biketrip_id, n.gid as end_taxizone_id
FROM tmp_points t, taxi_zones n
WHERE ST_Within(t.end_station, n.geom);
CREATE INDEX on tmp_ends_tz(biketrip_id);
EOF

wait

psql `cat ~/.sqlconninfo` <<EOF
DROP FOREIGN TABLE IF EXISTS bike_t;
CREATE FOREIGN TABLE bike_t(
    biketrip_id BIGINT,
    trip_duration INTEGER,
    start_time TIMESTAMP,
    stop_time TIMESTAMP,
    start_station_id INTEGER,
    start_station_name VARCHAR(60),
    start_station_latitude REAL,
    start_station_longitude REAL,
    end_station_id INTEGER,
    end_station_name VARCHAR(60),
    end_station_latitude REAL,
    end_station_longitude REAL,
    bike_id BIGINT,
    user_type VARCHAR(15),
    birth_year REAL,
    gender INTEGER,
    start_ct_id INTEGER,
    start_taxizone_id INTEGER,
    end_ct_id INTEGER,
    end_taxizone_id INTEGER
    ) SERVER cstore_server
OPTIONS(compression 'pglz');  

INSERT INTO bike_t 
  SELECT * FROM bike_ingest 
  FULL OUTER JOIN tmp_starts_ct USING (biketrip_id)
  FULL OUTER JOIN tmp_starts_tz USING (biketrip_id)
  FULL OUTER JOIN tmp_ends_ct USING (biketrip_id)
  FULL OUTER JOIN tmp_ends_tz USING (biketrip_id)
  ORDER BY start_station_id, start_time;
EOF

psql `cat ~/.sqlconninfo` <<EOF
DROP TABLE tmp_points CASCADE;
DROP TABLE tmp_starts_ct CASCADE;
DROP TABLE tmp_starts_tz CASCADE;
DROP TABLE tmp_ends_ct CASCADE;
DROP TABLE tmp_ends_tz CASCADE;
DROP TABLE bike_ingest;
EOF


