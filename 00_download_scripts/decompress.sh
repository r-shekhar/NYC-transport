#!/bin/bash

cd raw_data/bike

for x in *zip
do unzip $x
done

cd ../uber

unzip uber-raw-data-janjune-15.csv.zip

cd ../..
