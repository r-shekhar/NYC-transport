#!/bin/bash
set -e 

echo "Ingesting bike data."

rm -f bikefifo subwayfifo taxififo

mkfifo bikefifo && lzop -cd /bigdata/csv/citibike*.csv.lzo |grep -v start >> bikefifo &
psql `cat ~/.sqlconninfo` <<EOF 
DROP TABLE IF EXISTS bike_ingest;
CREATE TABLE bike_ingest(
    biketrip_id BIGSERIAL,
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
    gender INTEGER    
);

\copy bike_ingest(trip_duration, start_time, stop_time, start_station_id, start_station_name, start_station_latitude, start_station_longitude, end_station_id, end_station_name, end_station_latitude, end_station_longitude, bike_id,user_type, birth_year,gender) FROM bikefifo DELIMITERS ',' CSV;
EOF
rm bikefifo;
echo "Bike Data ingested."

echo "Ingesting Subway data."

# cat /bigdata/csv/subway*csv | sed -e 's/"//g' |grep --text -v unit > subwayfifo &
mkfifo subwayfifo
psql `cat ~/.sqlconninfo` <<EOF
DROP TABLE IF EXISTS subway_ingest;
CREATE TABLE subway_ingest(
    turnstile_id BIGSERIAL,
    endtime TIMESTAMP,
    ca VARCHAR(10),
    unit VARCHAR(10),
    scp VARCHAR(10),
    station VARCHAR(30),
    linename VARCHAR(15),
    division VARCHAR(15),
    description VARCHAR(15),
    cumul_entries BIGINT,
    cumul_exits BIGINT
);
EOF
for x in /bigdata/csv/subway*.csv.lzo
do echo $x;
lzop -cd $x | grep --text -v '"' > subwayfifo &
psql `cat ~/.sqlconninfo` <<EOF
\copy subway_ingest(endtime, ca, unit, scp, station, linename, division, description, cumul_entries, cumul_exits) FROM subwayfifo DELIMITERS ',' CSV HEADER;
EOF
done;
rm subwayfifo;

echo "Subway data ingested."

echo "Creating Taxi table."
psql `cat ~/.sqlconninfo` <<EOF
DROP TABLE IF EXISTS taxi_ingest;
CREATE TABLE taxi_ingest(
    trip_id BIGSERIAL,
    dropoff_datetime TIMESTAMP,
    dropoff_latitude REAL,
    dropoff_location_id REAL,
    dropoff_longitude REAL,
    ehail_fee REAL,
    extra REAL,
    fare_amount REAL,
    improvement_surcharge REAL,
    mta_tax REAL,
    passenger_count INTEGER,
    payment_type VARCHAR(10),
    pickup_datetime TIMESTAMP,
    pickup_latitude REAL,
    pickup_location_id REAL,
    pickup_longitude REAL,
    rate_code_id INTEGER,
    store_and_fwd_flag VARCHAR(10),
    tip_amount REAL,
    tolls_amount REAL,
    total_amount REAL,
    trip_distance REAL,
    trip_type VARCHAR(10),
    vendor_id VARCHAR(10)
);
DROP FOREIGN TABLE IF EXISTS taxi_ingest_col;
CREATE FOREIGN TABLE taxi_ingest_col(
    trip_id BIGINT,
    dropoff_datetime TIMESTAMP,
    dropoff_latitude REAL,
    dropoff_location_id REAL,
    dropoff_longitude REAL,
    ehail_fee REAL,
    extra REAL,
    fare_amount REAL,
    improvement_surcharge REAL,
    mta_tax REAL,
    passenger_count INTEGER,
    payment_type VARCHAR(10),
    pickup_datetime TIMESTAMP,
    pickup_latitude REAL,
    pickup_location_id REAL,
    pickup_longitude REAL,
    rate_code_id INTEGER,
    store_and_fwd_flag VARCHAR(10),
    tip_amount REAL,
    tolls_amount REAL,
    total_amount REAL,
    trip_distance REAL,
    trip_type VARCHAR(10),
    vendor_id VARCHAR(10)
) SERVER cstore_server
OPTIONS(compression 'pglz');

DROP FOREIGN TABLE IF EXISTS taxiloc_col;
CREATE FOREIGN TABLE taxiloc_col(
    trip_id BIGINT,
    dropoff_location_id REAL,
    pickup_location_id REAL,
    pickup_ct_id INTEGER,
    dropoff_ct_id INTEGER
)SERVER cstore_server
OPTIONS(compression 'pglz');
EOF

mkfifo taxififo
for x in /bigdata/csv/all*csv.lzo
do 
lzop -cd $x > taxififo &
psql `cat ~/.sqlconninfo` <<EOF
\copy taxi_ingest(dropoff_datetime, dropoff_latitude, dropoff_location_id, dropoff_longitude, ehail_fee, extra, fare_amount, improvement_surcharge, mta_tax, passenger_count, payment_type, pickup_datetime, pickup_latitude, pickup_location_id, pickup_longitude, rate_code_id, store_and_fwd_flag, tip_amount, tolls_amount, total_amount, trip_distance, trip_type, vendor_id) FROM taxififo DELIMITERS ',' CSV HEADER;
EOF
echo "Ingested $x. Performing spatial merge."

psql `cat ~/.sqlconninfo` <<EOF
DROP TABLE IF EXISTS tmp_points;
CREATE TABLE tmp_points AS
SELECT
  trip_id,
  ST_SetSRID(ST_MakePoint(pickup_longitude, pickup_latitude), 4326) as pickup,
  ST_SetSRID(ST_MakePoint(dropoff_longitude, dropoff_latitude), 4326) as dropoff
FROM taxi_ingest
WHERE (abs(pickup_longitude + 73.95) < 1.0 AND
       abs(pickup_latitude - 40.75) < 1.0 ) OR 
      (abs(dropoff_longitude + 73.95) < 1.0 AND
       abs(dropoff_latitude - 40.75) < 1.0 
      );
EOF

psql `cat ~/.sqlconninfo` <<EOF &
CREATE INDEX idx_tmp_points_start ON tmp_points USING gist (pickup);
EOF

psql `cat ~/.sqlconninfo` <<EOF &
CREATE INDEX idx_tmp_points_end ON tmp_points USING gist (dropoff);
EOF

wait

psql `cat ~/.sqlconninfo` <<EOF &
CREATE TABLE tmp_starts_ct AS
SELECT t.trip_id, n.gid as pickup_ct_id
FROM tmp_points t, nyct2010 n
WHERE ST_Within(t.pickup, n.geom);
CREATE INDEX on tmp_starts_ct(trip_id);
EOF

psql `cat ~/.sqlconninfo` <<EOF &
CREATE TABLE tmp_starts_tz AS
SELECT t.trip_id, n.gid as pickup_location_id
FROM tmp_points t, taxi_zones n
WHERE ST_Within(t.pickup, n.geom);
CREATE INDEX on tmp_starts_tz(trip_id);
EOF

psql `cat ~/.sqlconninfo` <<EOF &
CREATE TABLE tmp_ends_ct AS
SELECT t.trip_id, n.gid as dropoff_ct_id
FROM tmp_points t, nyct2010 n
WHERE ST_Within(t.dropoff, n.geom);
CREATE INDEX on tmp_ends_ct(trip_id);
EOF

psql `cat ~/.sqlconninfo` <<EOF &
CREATE TABLE tmp_ends_tz AS
SELECT t.trip_id, n.gid as dropoff_location_id
FROM tmp_points t, taxi_zones n
WHERE ST_Within(t.dropoff, n.geom);
CREATE INDEX on tmp_ends_tz(trip_id);
EOF

wait

psql `cat ~/.sqlconninfo` <<EOF
INSERT INTO taxiloc_col
    select * from tmp_starts_ct
    FULL OUTER JOIN tmp_starts_tz USING (trip_id)
    FULL OUTER JOIN tmp_ends_ct USING (trip_id)
    FULL OUTER JOIN tmp_ends_tz USING (trip_id);
INSERT INTO taxi_ingest_col
    SELECT * FROM taxi_ingest;
EOF

# psql `cat ~/.sqlconninfo` <<EOF
# CREATE TABLE mergeloc AS select * from tmp_starts_ct
# FULL OUTER JOIN tmp_starts_tz USING (trip_id)
# FULL OUTER JOIN tmp_ends_ct USING (trip_id)
# FULL OUTER JOIN tmp_ends_tz USING (trip_id);
# CREATE INDEX ON mergeloc(trip_id);

# UPDATE taxi_ingest 
# SET 
#     pickup_ct_id = n.pickup_ct_id,
#     dropoff_ct_id = n.dropoff_ct_id,
#     pickup_location_id = n.pickup_location_id,
#     dropoff_location_id = n.dropoff_location_id
# FROM (SELECT 
#     ti.trip_id, 
#     COALESCE(ti.pickup_ct_id, v2.pickup_ct_id) as pickup_ct_id,
#     COALESCE(ti.pickup_location_id, v2.pickup_location_id) as pickup_location_id,
#     COALESCE(ti.dropoff_ct_id, v2.dropoff_ct_id) as dropoff_ct_id,
#     COALESCE(ti.dropoff_location_id, v2.dropoff_location_id) as dropoff_location_id
#  FROM taxi_ingest ti, mergeloc v2 WHERE ti.trip_id=v2.trip_id) as n
# WHERE n.trip_id=taxi_ingest.trip_id;

# INSERT INTO taxi_ingest_col
#     SELECT * FROM taxi_ingest;
# EOF


psql `cat ~/.sqlconninfo` <<EOF
TRUNCATE TABLE taxi_ingest;
DROP INDEX idx_tmp_points_start;
DROP INDEX idx_tmp_points_end;
DROP TABLE tmp_points CASCADE;
DROP TABLE tmp_starts_ct CASCADE;
DROP TABLE tmp_starts_tz CASCADE;
DROP TABLE tmp_ends_ct CASCADE;
DROP TABLE tmp_ends_tz CASCADE;
EOF

echo "Spatial merge of $x complete."
done

rm taxififo
echo "Taxi table ingest complete"

psql `cat ~/.sqlconninfo` <<EOF
DROP TABLE taxi_ingest;
ANALYZE taxi_ingest_col;
EOF