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
EOF

mkfifo taxififo
for x in /bigdata/csv/all*csv.lzo
do echo "Processing ${x}";
lzop -cd $x > taxififo &
echo "Ingesting $x"
psql `cat ~/.sqlconninfo` <<EOF
\copy taxi_ingest(dropoff_datetime, dropoff_latitude, dropoff_location_id, dropoff_longitude, ehail_fee, extra, fare_amount, improvement_surcharge, mta_tax, passenger_count, payment_type, pickup_datetime, pickup_latitude, pickup_location_id, pickup_longitude, rate_code_id, store_and_fwd_flag, tip_amount, tolls_amount, total_amount, trip_distance, trip_type, vendor_id) FROM taxififo DELIMITERS ',' CSV HEADER;
EOF
echo "Ingested $x."
sleep 1;
done

rm taxififo
echo "Taxi table ingest complete"

