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


INSERT INTO bike_ingest_col 
  SELECT * FROM bike_ingest ORDER BY start_station_id, start_time;
INSERT INTO subway_ingest_col
  SELECT * FROM subway_ingest ORDER BY ca, unit, scp, endtime;


ANALYZE bike_ingest_col;
ANALYZE subway_ingest_col;

DROP TABLE bike_ingest;
DROP TABLE subway_ingest;
EOF
