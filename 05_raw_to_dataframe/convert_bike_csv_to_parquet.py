#!/usr/bin/env python
# coding: utf-8

from dask.distributed import Client
from glob import glob
import dask.dataframe as dd
import fastparquet
import json
import numpy as np
import os
import os.path


schema = """trip_duration,start_time,stop_time,start_station_id,
start_station_name,start_station_latitude,start_station_longitude,
end_station_id,end_station_name,end_station_latitude,
end_station_longitude,bike_id,user_type,birth_year,gender""".split(',')
schema = [x.strip() for x in schema]

with open('config.json', 'r') as fh:
    config = json.load(fh)


def main(client):
    df = dd.read_csv(
        glob(os.path.join(config["citibike_raw_data_path"], '2*iti*.csv')),
        # compression='gzip',
        parse_dates=[1, 2, ],
        infer_datetime_format=True,
        # blocksize=500*(2**20),
        na_values=["\\N"],
        header=0,
        names=schema,
        dtype={
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
    df = df.repartition(npartitions=32)

    # This is buggy -- The file cannot always be read back in. There must be
    # a race condition somewhere. Instead we load df into memory and use
    # fastparquet directly (instead of through dask layer) below.
    # df.to_parquet('/data3/citibike.parq', compression="SNAPPY")

    df = df.compute()

    p = os.path.join(config['hdf_output_path'], 'citibike.hdf')
    df.to_hdf(p, '/data', complevel=1, complib='blosc')

    fastparquet.write(
        os.path.join(config['parquet_output_path'], 'citibike.parquet'),
        df,
        row_group_offsets=100000,
        has_nulls=True,
        file_scheme='hive',
        compression='SNAPPY',
        object_encoding='utf8')

    df.to_csv(os.path.join(config['csv_output_path'], 'citibike.csv'),
        float_format='%.8g',
        )


if __name__ == '__main__':
    client = Client()
    main(client)
