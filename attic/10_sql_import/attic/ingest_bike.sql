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

-- DROP TABLE IF EXISTS bike_stations
-- CREATE TABLE bike_stations(
--   id integer primary key,
--   latitude real,
--   longitude real,
--   stationName VARCHAR,
--   statusValue VARCHAR,
--   totalDocks integer
-- );

-- \copy bike_stations(id,latitude,longitude,stationName,statusValue,totalDocks) FROM 'stations-2017.04.18.csv' CSV HEADER;

-- CREATE TABLE station_points AS SELECT id, latitude, longitude, start_time, name FROM 
--     (SELECT distinct start_station_id AS id, start_station_latitude AS latitude,
--      start_station_longitude AS longitude, start_time, start_station_name as name
--       FROM bike_ingest 
--      UNION  
--      SELECT distinct end_station_id AS id, end_station_latitude AS latitude, 
--      end_station_longitude AS longitude, start_time, start_station_name as name
--       FROM bike_ingest) AS subquery;

-- CREATE TABLE avg_coord_per_id AS SELECT DISTINCT on (id) id, 
--     AVG(latitude) AS latitude, AVG(longitude) AS longitude 
--     FROM station_points s WHERE abs(s.longitude + 74.0) < 0.5 AND abs(s.latitude - 40.7) < 0.5 
--     group by id order by id;

-- SELECT c.id, c.latitude, c.longitude, sq.name from avg_coord_per_id c
--  INNER JOIN (SELECT DISTINCT id, name from station_points s) as sq
--  ON sq.id = c.id;

-- CREATE INDEX on bike_stations(id);
-- SELECT AddGeometryColumn('bike_stations', 'location', 4326, 'POINT', 2);

-- UPDATE bike_stations
-- SET 
--     location=ST_SetSRID(ST_MakePoint(
--             subquery.longitude, 
--             subquery.latitude), 4326)
-- FROM (SELECT id, longitude, latitude FROM bike_stations) as subquery
-- WHERE bike_stations.id=subquery.id;

-- ALTER TABLE bike_stations ADD COLUMN taxi_zone_id integer;

-- CREATE INDEX idx_bike_station_points on bike_stations USING gist(location);

-- DROP TABLE station_points;
-- DROP TABLE latest_trip_per_id;

-- CREATE TABLE tmp_stations AS

-- UPDATE bike_stations
-- SET
--     taxi_zone_id = subquery.gid
-- FROM (SELECT bs.id, tz.gid FROM bike_stations bs, taxi_zones tz
--       WHERE ST_Within(bs.location, tz.geom)) as subquery
-- WHERE bike_stations.id = subquery.id;
