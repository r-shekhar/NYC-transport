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

lock = SerializableLock()


with open('config.json', 'r') as fh:
    config = json.load(fh)


def glob(x):
    from glob import glob
    return sorted(glob(x))


def main(client):

    # Define schemas
    green_schema_pre_2015="vendor_id,pickup_datetime,dropoff_datetime,store_and_fwd_flag,rate_code_id,pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude,passenger_count,trip_distance,fare_amount,extra,mta_tax,tip_amount,tolls_amount,ehail_fee,total_amount,payment_type,trip_type,junk1,junk2"
    green_glob_pre_2015 = glob(os.path.join(config['taxi_raw_data_path'],'green_tripdata_201[34]*.csv'))

    green_schema_2015_h1="vendor_id,pickup_datetime,dropoff_datetime,store_and_fwd_flag,rate_code_id,pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude,passenger_count,trip_distance,fare_amount,extra,mta_tax,tip_amount,tolls_amount,ehail_fee,improvement_surcharge,total_amount,payment_type,trip_type,junk1,junk2"
    green_glob_2015_h1 = glob(os.path.join(config['taxi_raw_data_path'],'green_tripdata_2015-0[1-6].csv'))

    green_schema_2015_h2_2016_h1="vendor_id,pickup_datetime,dropoff_datetime,store_and_fwd_flag,rate_code_id,pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude,passenger_count,trip_distance,fare_amount,extra,mta_tax,tip_amount,tolls_amount,ehail_fee,improvement_surcharge,total_amount,payment_type,trip_type"
    green_glob_2015_h2_2016_h1 = glob(os.path.join(config['taxi_raw_data_path'],'green_tripdata_2015-0[7-9].csv')) + glob(os.path.join(config['taxi_raw_data_path'],'green_tripdata_2015-1[0-2].csv')) + glob(os.path.join(config['taxi_raw_data_path'],'green_tripdata_2016-0[1-6].csv'))

    green_schema_2016_h2="vendor_id,pickup_datetime,dropoff_datetime,store_and_fwd_flag,rate_code_id,pickup_location_id,dropoff_location_id,passenger_count,trip_distance,fare_amount,extra,mta_tax,tip_amount,tolls_amount,ehail_fee,improvement_surcharge,total_amount,payment_type,trip_type,junk1,junk2"
    green_glob_2016_h2 = glob(os.path.join(config['taxi_raw_data_path'],'green_tripdata_2016-0[7-9].csv')) + glob(os.path.join(config['taxi_raw_data_path'],'green_tripdata_2016-1[0-2].csv'))

    yellow_schema_pre_2015="vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,pickup_longitude,pickup_latitude,rate_code_id,store_and_fwd_flag,dropoff_longitude,dropoff_latitude,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,total_amount"
    yellow_glob_pre_2015 = glob(os.path.join(config['taxi_raw_data_path'],'yellow_tripdata_201[0-4]*.csv'))

    yellow_schema_2015_2016_h1="vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,pickup_longitude,pickup_latitude,rate_code_id,store_and_fwd_flag,dropoff_longitude,dropoff_latitude,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount"
    yellow_glob_2015_2016_h1 = glob(os.path.join(config['taxi_raw_data_path'],'yellow_tripdata_2015*.csv')) + glob(os.path.join(config['taxi_raw_data_path'],'yellow_tripdata_2016-0[1-6].csv'))

    yellow_schema_2016_h2="vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,store_and_fwd_flag,pickup_location_id,dropoff_location_id,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,junk1,junk2"
    yellow_glob_2016_h2 = glob(os.path.join(config['taxi_raw_data_path'],'yellow_tripdata_2016-0[7-9].csv')) + glob(os.path.join(config['taxi_raw_data_path'],'yellow_tripdata_2016-1[0-2].csv'))

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
    #     'dropoff_datetime': object, # set by parse_dates in pandas read_csv
        'dropoff_latitude': np.float32,
        'dropoff_location_id': np.int32,
        'dropoff_longitude': np.float32,
        'ehail_fee': np.float32,
        'extra': np.float32,
        'fare_amount': np.float32,
        'improvement_surcharge': np.float32,
        'junk1': object,
        'junk2': object,
        'mta_tax': np.float32,
        'passenger_count': np.int32,
        'payment_type': object,
    #     'pickup_datetime': object, # set by parse_dates in pandas read_csv
        'pickup_latitude': np.float32,
        'pickup_location_id': np.int32,
        'pickup_longitude': np.float32,
        'rate_code_id': np.int32,
        'store_and_fwd_flag': object,
        'tip_amount': np.float32,
        'tolls_amount': np.float32,
        'total_amount': np.float32,
        'trip_distance': np.float32,
        'trip_type': object,
        'vendor_id': object
    }


    ## Green
    green1 = dd.read_csv(green_glob_pre_2015, header=0, 
                         na_values=["NA"], 
                         parse_dates=[1,2],
                         infer_datetime_format = True,
                         dtype=dtype_list,
                         names=green_schema_pre_2015.split(','))
    green1['dropoff_location_id'] = green1['rate_code_id'].copy()
    green1['dropoff_location_id'] = -999
    green1['pickup_location_id'] = green1['rate_code_id'].copy()
    green1['pickup_location_id'] = -999
    green1['improvement_surcharge'] = green1['total_amount'].copy()
    green1['improvement_surcharge'] = np.nan
    green1 = green1.drop(['junk1', 'junk2'], axis=1)


    green2 = dd.read_csv(green_glob_2015_h1, header=0, 
                         na_values=["NA"],
                         parse_dates=[1,2],
                         infer_datetime_format = True,
                         dtype=dtype_list,
                         names=green_schema_2015_h1.split(','))
    green2['dropoff_location_id'] = green2['rate_code_id'].copy()
    green2['dropoff_location_id'] = -999
    green2['pickup_location_id'] = green2['rate_code_id'].copy()
    green2['pickup_location_id'] = -999
    green2 = green2.drop(['junk1', 'junk2'], axis=1)


    green3 = dd.read_csv(green_glob_2015_h2_2016_h1, header=0, 
                         na_values=["NA"],
                         parse_dates=[1,2],
                         infer_datetime_format = True,
                         dtype=dtype_list,
                         names=green_schema_2015_h2_2016_h1.split(','))
    green3['dropoff_location_id'] = green3['rate_code_id'].copy()
    green3['dropoff_location_id'] = -999
    green3['pickup_location_id'] = green3['rate_code_id'].copy()
    green3['pickup_location_id'] = -999


    green4 = dd.read_csv(green_glob_2016_h2, header=0, 
                         na_values=["NA"],
                         parse_dates=[1,2],
                         infer_datetime_format = True,
                         dtype=dtype_list,
                         names=green_schema_2016_h2.split(','))
    green4['dropoff_latitude'] = green4['total_amount'].copy()
    green4['dropoff_latitude'] = np.nan
    green4['dropoff_longitude'] = green4['total_amount'].copy()
    green4['dropoff_longitude'] = np.nan
    green4['pickup_latitude'] = green4['total_amount'].copy()
    green4['pickup_latitude'] = np.nan
    green4['pickup_longitude'] = green4['total_amount'].copy()
    green4['pickup_longitude'] = np.nan
    green4 = green4.drop(['junk1', 'junk2'], axis=1)


    green = green1[sorted(green1.columns)].append(
        green2[sorted(green1.columns)])
    green = green.append(green3[sorted(green1.columns)])
    green = green.append(green4[sorted(green1.columns)])
    green = green.repartition(npartitions=20)

    # green = client.persist(green)

    ## To_hdf is well tested and works, but unfortunately is really slow to 
    ## load into postgresql through Pandas. 
    # green.to_hdf(
    #     os.path.join(config['hdf_output_path'],'green-*.hdf'), 
    #     '/data', complib='blosc', 
    #     complevel=1, lock=lock)


    ## To_parquet is currently (2017 Feb) alpha software, and seems to create
    ## bad files where some data is corrupted when reading the files back in.
    # green.to_parquet('/data3/green.parq', compression="SNAPPY", has_nulls=True,
    #     object_encoding='json')

    ## Sadly this is the only format that works flawlessly.
    # green.to_csv(os.path.join(config['csv_output_path'], 'green-*.csv'))

    #GREEN09 (or 08) seem to be the issue

    green_hdf_files = glob(
        os.path.join(config['hdf_output_path'], 'green-*.hdf'))

    for i, f in enumerate(green_hdf_files):
        print( (i, f))
        df = pd.read_hdf(f)
        
        if i==0:
            fastparquet.write(
                os.path.join(config['parquet_output_path'], "green.parquet"),
                df,
                compression="SNAPPY",
                file_scheme="hive",
                has_nulls=True,
                object_encoding='utf8')
            # df.to_hdf(
            #     os.path.join(config['hdf_output_path'], "green-all.hdf"),
            #     complevel=1, complib='blosc', format='t'
            #     )

        else:
            fastparquet.write(
                os.path.join(config['parquet_output_path'], "green.parquet"),
                df, append=True,
                compression="SNAPPY",
                file_scheme="hive",
                has_nulls=True,
                write_index=True,
                object_encoding='utf8')
            # df.to_hdf(
            #     os.path.join(config['hdf_output_path'], "green-all.hdf"),
            #     complevel=1, complib='blosc', format='t', append=True
            #     )

    sys.exit(9)

    #----------------------------------------------------------------------

    # # Yellow
    yellow1 = dd.read_csv(yellow_glob_pre_2015, header=0, 
                         na_values=["NA"],
                         parse_dates=[1,2],
                         infer_datetime_format = True,
                         dtype=dtype_list,
                         names=yellow_schema_pre_2015.split(','))
    yellow1['dropoff_location_id'] = yellow1['rate_code_id'].copy()
    yellow1['dropoff_location_id'] = -999
    yellow1['pickup_location_id'] = yellow1['rate_code_id'].copy()
    yellow1['pickup_location_id'] = -999
    yellow1['ehail_fee'] = yellow1['total_amount'].copy()
    yellow1['ehail_fee'] = np.nan
    yellow1['improvement_surcharge'] = yellow1['total_amount'].copy()
    yellow1['improvement_surcharge'] = np.nan
    yellow1['trip_type'] = yellow1['rate_code_id'].copy()
    yellow1['trip_type'] = -999


    yellow2 = dd.read_csv(yellow_glob_2015_2016_h1, header=0, 
                         na_values=["NA"],
                         parse_dates=[1,2],
                         infer_datetime_format = True,
                         dtype=dtype_list,
                         names=yellow_schema_2015_2016_h1.split(','))
    yellow2['dropoff_location_id'] = yellow2['rate_code_id'].copy()
    yellow2['dropoff_location_id'] = -999
    yellow2['pickup_location_id'] = yellow2['rate_code_id'].copy()
    yellow2['pickup_location_id'] = -999
    yellow2['ehail_fee'] = yellow2['total_amount'].copy()
    yellow2['ehail_fee'] = np.nan
    yellow2['trip_type'] = yellow2['rate_code_id'].copy()
    yellow2['trip_type'] = -999


    yellow3 = dd.read_csv(yellow_glob_2016_h2, header=0, 
                         na_values=["NA"],
                         parse_dates=[1,2],
                         infer_datetime_format = True,
                         dtype=dtype_list,
                         names=yellow_schema_2016_h2.split(','))
    yellow3['dropoff_latitude'] = yellow3['total_amount'].copy()
    yellow3['dropoff_latitude'] = np.nan
    yellow3['dropoff_longitude'] = yellow3['total_amount'].copy()
    yellow3['dropoff_longitude'] = np.nan
    yellow3['pickup_latitude'] = yellow3['total_amount'].copy()
    yellow3['pickup_latitude'] = np.nan
    yellow3['pickup_longitude'] = yellow3['total_amount'].copy()
    yellow3['pickup_longitude'] = np.nan
    yellow3['ehail_fee'] = yellow3['total_amount'].copy()
    yellow3['ehail_fee'] = np.nan
    yellow3['trip_type'] = yellow3['rate_code_id'].copy()
    yellow3['trip_type'] = -999
    yellow3 = yellow3.drop(['junk1', 'junk2'], axis=1)


    yellow = yellow1[sorted(yellow1.columns)].append(
        yellow2[sorted(yellow1.columns)])
    yellow = yellow.append(yellow3[sorted(yellow1.columns)])

    yellow = yellow.repartition(npartitions=500)
    # yellow = client.persist(yellow)

    ## To_hdf is well tested and works, but unfortunately is really slow to 
    ## load into postgresql through Pandas. 
    yellow.to_hdf(
        os.path.join(config['hdf_output_path'],'yellow-*.hdf'), 
        '/data', complib='blosc', 
        complevel=1, lock=lock)


    ## To_parquet is currently (2017 Feb) alpha software, and seems to create
    ## bad files where some data is corrupted when reading the files back in.
    # yellow.to_parquet('/data3/yellow.parq', compression="SNAPPY", has_nulls=True,
    #     object_encoding='json')

    ## Sadly this is the only format that works flawlessly.
    yellow.to_csv(os.path.join(config['csv_output_path'], 'yellow-*.csv'))

    yellow_hdf_files = glob(
        os.path.join(config['hdf_output_path'], 'yellow-*.hdf'))

    for i, f in enumerate(yellow_hdf_files):
        print( (i, f))
        df = pd.read_hdf(f)
        
        if i==0:
            fastparquet.write(
                os.path.join(config['parquet_output_path'], "yellow.parq"),
                df,
                compression="SNAPPY",
                file_scheme="hive",
                has_nulls=True,
                write_index=True,
                object_encoding='utf8')
            df.to_hdf(
                os.path.join(config['hdf_output_path'], "yellow-all.hdf"),
                complevel=1, complib='blosc'
                )
        else:
            fastparquet.write(
                os.path.join(config['parquet_output_path'], "yellow.parq"),
                df, append=True,
                compression="SNAPPY",
                file_scheme="hive",
                has_nulls=True,
                write_index=True,
                object_encoding='utf8')
            df.to_hdf(
                os.path.join(config['hdf_output_path'], "yellow-all.hdf"),
                complevel=1, complib='blosc', append=True
                )




if __name__ == '__main__':
    client = Client()
    main(client)

