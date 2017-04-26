#!/bin/bash
set -e 

psql `cat ~/.sqlconninfo` <<EOF 

DROP FOREIGN TABLE IF EXISTS bike_ingest_col;
CREATE FOREIGN TABLE bike_ingest_col(
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
    gender INTEGER    
) SERVER cstore_server
OPTIONS(compression 'pglz');

DROP FOREIGN TABLE IF EXISTS subway_ingest_col;
CREATE FOREIGN TABLE subway_ingest_col(
    turnstile_id BIGINT,
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
) SERVER cstore_server
OPTIONS(compression 'pglz');

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

INSERT INTO bike_ingest_col 
  SELECT * FROM bike_ingest ORDER BY start_station_id, start_time;
INSERT INTO subway_ingest_col
  SELECT * FROM subway_ingest ORDER BY ca, unit, endtime;
INSERT INTO taxi_ingest_col
  SELECT * from taxi_ingest ORDER BY pickup_datetime;

EOF
