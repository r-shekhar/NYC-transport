#!/usr/bin/env python
# coding: utf-8

from dask import delayed
from dask.distributed import Client
from dask.utils import SerializableLock

import dask.dataframe as dd
import fastparquet
import json
import numpy as np
import pandas as pd
import os, sys


with open('config.json', 'r') as fh:
    config = json.load(fh)

def glob(x):
    from glob import glob
    return sorted(glob(x))

def trymakedirs(path):
    try:
        os.makedirs(path)
    except:
        pass



def main(client):

    # Define schemas
    uber_schema_2014="pickup_datetime,pickup_latitude,pickup_longitude,junk1"
    uber_glob_2014 = glob(os.path.join(config['uber_raw_data_path'],'uber*-???14.csv'))

    uber_schema_2015="junk1,pickup_datetime,junk2,pickup_location_id"
    uber_glob_2015 = glob(os.path.join(config['uber_raw_data_path'],'uber*15.csv'))

    ## Uncomment this block to get a printout of fields in the csv files
    # x=0
    # s = []
    # for x in [x for x in locals() if 'schema' in x]:
    #     s.append(set((locals()[x]).split(',')))
    # s = sorted(set.union(*s))
    # dtype_list = dict(zip(s, [object,]*len(s)))
    # print(dtype_list)


    # ### Actually declare the dtypes I want to use 

    dtype_list = { 
        'dropoff_datetime': np.int64,
        'dropoff_latitude': np.float64,
        'dropoff_location_id': np.int64,
        'dropoff_longitude': np.float64,
        'ehail_fee': np.float64,
        'extra': np.float64,
        'fare_amount': np.float64,
        'improvement_surcharge': np.float64,
        'junk1': object,
        'junk2': object,
        'mta_tax': np.float64,
        'passenger_count': np.int64,
        'payment_type': object,
    #     'pickup_datetime': object, # set by parse_dates in pandas read_csv
        'pickup_latitude': np.float64,
        'pickup_location_id': np.int64,
        'pickup_longitude': np.float64,
        'rate_code_id': np.int64,
        'store_and_fwd_flag': object,
        'tip_amount': np.float64,
        'tolls_amount': np.float64,
        'total_amount': np.float64,
        'trip_distance': np.float64,
        'trip_type': object,
        'vendor_id': object
    }

    ## Green
    uber1 = dd.read_csv(uber_glob_2014, header=0,
                         na_values=["NA"], 
                         parse_dates=[0,],
                         infer_datetime_format = True,
                         dtype=dtype_list,
                         names=uber_schema_2014.split(','))
    uber1 = uber1.drop(['junk1',], axis=1)
    uber1 = uber1.assign(pickup_location_id=-999)

    uber2 = dd.read_csv(uber_glob_2015, header=0,
                         na_values=["NA"], 
                         parse_dates=[1,],
                         infer_datetime_format = True,
                         dtype=dtype_list,
                         names=uber_schema_2015.split(','))
    uber2 = uber2.drop(['junk1', 'junk2'], axis=1)
    uber2 = uber2.assign(pickup_latitude=np.nan, pickup_longitude=np.nan)

    uber1 = uber1[sorted(uber1.columns)]
    uber2 = uber2[sorted(uber2.columns)]

    uberdf = uber1.append(uber2)

    default_values = {np.float64: np.nan, np.int64: -999, object: ""}


    for field in dtype_list:
        if (field in uberdf.columns):
            uberdf[field] = uberdf[field].astype(dtype_list[field])
        elif field == 'pickup_datetime':
            pass
        else:
            uberdf = uberdf.assign(**{field: default_values[dtype_list[field]]})

    uberdf = uberdf.drop(['junk1', 'junk2'], axis=1)
    #uberdf = uberdf.repartition(npartitions=20)

    uberdf = uberdf[sorted(uberdf.columns)]

    trymakedirs(os.path.join(config['parquet_output_path']))
    uberdf.to_parquet(
        os.path.join(config['parquet_output_path'], 'uber.parquet'),
        compression="SNAPPY", 
        has_nulls=True,
        object_encoding='json')


if __name__ == '__main__':
    client = Client()
    main(client)

