#!/bin/bash
set -e

psql `cat ~/.sqlconninfo` <<EOF
CREATE EXTENSION postgis;
EOF
shp2pgsql -s 2263:4326 ../taxi_zones/taxi_zones.shp | psql `cat ~/.sqlconninfo`

psql `cat ~/.sqlconninfo` <<EOF
CREATE EXTENSION postgis;

CREATE TABLE tmp_points_start AS
SELECT
  biketrip_id,
  ST_SetSRID(ST_MakePoint(start_station_longitude, start_station_latitude), 4326) as startpoint
FROM bike_ingest
WHERE abs(start_station_longitude + 74) < 0.5 AND abs(start_station_latitude - 40.75) < 0.5;

CREATE TABLE tmp_points_end AS
SELECT
  biketrip_id,
  ST_SetSRID(ST_MakePoint(end_station_longitude, end_station_latitude), 4326) as endpoint
FROM bike_ingest
WHERE abs(end_station_longitude + 74) < 0.5 AND abs(end_station_latitude - 40.75) < 0.5;


CREATE INDEX idx_tmp_start ON tmp_points_start USING gist (startpoint);
CREATE INDEX idx_tmp_end ON tmp_points_end USING gist (endpoint);

CREATE TABLE tmp_pickups AS
SELECT t.id, n.gid
FROM tmp_points t, nyct2010 n
WHERE ST_Within(t.pickup, n.geom);

CREATE TABLE tmp_dropoffs AS
SELECT t.id, n.gid
FROM tmp_points t, nyct2010 n
WHERE ST_Within(t.dropoff, n.geom);
-- Bike

SELECT AddGeometryColumn('bike_ingest', 'start_point', 4326, 'POINT', 2);
SELECT AddGeometryColumn('bike_ingest', 'end_point', 4326, 'POINT', 2); 

CREATE TABLE tmp_points_start AS
SELECT
  biketrip_id,
  ST_SetSRID(ST_MakePoint(start_station_longitude, start_station_latitude), 4326) as startpoint
FROM bike_ingest
WHERE abs(start_station_longitude + 74) < 0.5 AND abs(start_station_latitude - 40.75) < 0.5;

CREATE TABLE tmp_points_end AS
SELECT
  biketrip_id,
  ST_SetSRID(ST_MakePoint(end_station_longitude, end_station_latitude), 4326) as endpoint
FROM bike_ingest
WHERE abs(end_station_longitude + 74) < 0.5 AND abs(end_station_latitude - 40.75) < 0.5;


UPDATE bike_ingest
SET 
    start_point=ST_SetSRID(ST_MakePoint(
            subquery.start_station_longitude, 
            subquery.start_station_latitude), 4326)
FROM (SELECT 
    biketrip_id, 
    start_station_longitude,
    start_station_latitude
FROM bike_ingest) as subquery
WHERE bike_ingest.biketrip_id=subquery.biketrip_id;

UPDATE bike_ingest
SET 
    end_point=ST_SetSRID(ST_MakePoint(
            subquery.end_station_longitude, 
            subquery.end_station_latitude), 4326)
FROM (SELECT 
    biketrip_id, 
    end_station_longitude,
    end_station_latitude
FROM bike_ingest) as subquery
WHERE bike_ingest.biketrip_id=subquery.biketrip_id;

-- executed up to here

CREATE INDEX idx_bike_points_start ON bike_ingest USING gist (start_point);
CREATE INDEX idx_bike_points_start ON bike_ingest USING gist (end_point);



-- A more normalized approach might be better, but this is not too costly
-- (a couple of minutes of cpu time, one time)

CREATE TABLE tmp_points AS
SELECT 
 biketrip_id,
 ST_SetSRID(ST_MakePoint(start_station_longitude, start_station_latitude), 4326) as start_point,
 ST_SetSRID(ST_MakePoint(end_station_longitude, end_station_latitude), 4326) as end_point    
FROM bike_ingest
WHERE start_station_longitude IS NOT NULL  OR start_station_longitude != 0.0
      OR end_station_longitude IS NOT NULL OR end_station_longitude != 0.0;

-- Taxi Data
SELECT AddGeometryColumn('trip_ingest', 'start_point', 4326, 'POINT', 2);
SELECT AddGeometryColumn('trip_ingest', 'end_point', 4326, 'POINT', 2); 

CREATE TABLE tmp_points AS
SELECT
  id,
  ST_SetSRID(ST_MakePoint(pickup_longitude, pickup_latitude), 4326) as pickup,
  ST_SetSRID(ST_MakePoint(dropoff_longitude, dropoff_latitude), 4326) as dropoff
FROM trip_ingest
WHERE pickup_longitude IS NOT NULL OR dropoff_longitude IS NOT NULL;



EOF