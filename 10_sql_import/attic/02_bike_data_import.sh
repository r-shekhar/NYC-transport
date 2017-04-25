#!/bin/bash
set -e 

echo "Ingesting bike data."

mkfifo bikefifo && zcat /bigdata/csv/citibike*csv.gz |grep -v start >> bikefifo &
psql `cat ~/.sqlconninfo` < ingest_bike.sql
rm bikefifo;
echo "Bike Data ingested."

