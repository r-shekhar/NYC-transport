#!/usr/bin/env python
# coding: utf-8

from dask.distributed import Client
import dask.dataframe as dd
import numpy as np
import pandas as pd

schema = """trip_duration,start_time,stop_time,start_station_id,
start_station_name,start_station_latitude,start_station_longitude,
end_station_id,end_station_name,end_station_latitude,
end_station_longitude,bike_id,user_type,birth_year,gender""".split(',')
schema = [x.strip() for x in schema]


def main():
    df = dd.read_csv('../00_download_scripts/raw_data/bike/2*iti*.csv',
                     #compression='gzip', 
                     parse_dates=[1,2,], 
                     blocksize=500*(2**20), 
                     na_values=["\\N"],
                     header=0,
                     names=schema,
                     dtype = {
                         'trip_duration':                     np.int32,
                         'start_station_id':                  np.int32,
                         'start_station_name':                object,
                         'start_station_latitude':            np.float32,
                         'start_station_longitude':           np.float32,
                         'end_station_id':                    np.int32,
                         'end_station_name':                  object,
                         'end_station_latitude':              np.float32,
                         'end_station_longitude':             np.float32,
                         'bike_id':                           np.int32,
                         'user_type':                         object,
                         'birth_year':                        np.float32,
                         'gender':                            np.int32,
                     }
    )

    df.dtypes

    df.head()

    #df = client.persist(df.set_index("start_time", npartitions=64))
    df.to_parquet('/data2/citibike.parq', compression="SNAPPY") 

    del df
    df = dd.read_parquet('/data2/citibike.parq')

    df.head()

if __name__ == '__main__':
    client = Client()
    main()
