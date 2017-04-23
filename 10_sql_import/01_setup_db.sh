#!/bin/bash

echo 'This script assumes it is running as the postgres user on ubuntu. '
echo 'It might need modifications when running on non ubuntu distributions. '


if [ "$(whoami)" != "postgres" ]; then
        echo "Script must be run as user: postgres"
        exit -1
fi

dropdb transitproject --if-exists
dropuser transituser --if-exists

psql <<EOF
create database transitproject;
create user transituser;
\c transitproject
grant all privileges on all tables in schema public to transituser;
alter role transituser with encrypted password 'yourpassword';
CREATE extension postgis;
CREATE extension cstore_fdw;
EOF

shp2pgsql -s 2263:4326 ../shapefiles/nyct2010.shp | psql -d transitproject
shp2pgsql -s 2263:4326 ../shapefiles/taxi_zones.shp | psql -d transitproject

psql -d transitproject<<EOF
CREATE INDEX idx_taxi_zones_geom ON taxi_zones USING gist (geom);
CREATE INDEX idx_taxi_zone_id ON taxi_zones (locationid);

grant all privileges on all tables in schema public to transituser;
GRANT SELECT ON public.geometry_columns TO transituser;
EOF