#!/usr/bin/env python
# coding: utf-8

import dask
from dask import delayed
from dask.distributed import Client

import dask.dataframe as dd
import json
import numpy as np
import pandas as pd
import geopandas
from shapely.geometry import Point
import os
import sys
import joblib


dtype_list = {
    #     'dropoff_datetime': object, # set by parse_dates in pandas read_csv
    'dropoff_latitude': np.float64,
    'dropoff_location_id': np.float64,
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
    'pickup_location_id': np.float64,
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


def assign_taxi_zones(df, lon_var, lat_var, locid_var):
    """Joins DataFrame with Taxi Zones shapefile.

    This function takes longitude values provided by `lon_var`, and latitude
    values provided by `lat_var` in DataFrame `df`, and performs a spatial join
    with the NYC taxi_zones shapefile. 

    The shapefile is hard coded in, as this function makes a hard assumption of
    latitude and longitude coordinates. It also assumes latitude=0 and 
    longitude=0 is not a datapoint that can exist in your dataset. Which is 
    reasonable for a dataset of New York, but bad for a global dataset.

    Only rows where `df.lon_var`, `df.lat_var` are reasonably near New York,
    and `df.locid_var` is set to np.nan are updated. 

    Parameters
    ----------
    df : pandas.DataFrame or dask.DataFrame
        DataFrame containing latitudes, longitudes, and location_id columns.
    lon_var : string
        Name of column in `df` containing longitude values. Invalid values 
        should be np.nan.
    lat_var : string
        Name of column in `df` containing latitude values. Invalid values 
        should be np.nan
    locid_var : string
        Name of column in `df` containing taxi_zone location ids. Rows with
        valid, nonzero values are not overwritten. 
    """

    localdf = df[[lon_var, lat_var, locid_var]].copy()
    # localdf = localdf.reset_index()
    localdf[lon_var] = localdf[lon_var].fillna(value=0.)
    localdf[lat_var] = localdf[lat_var].fillna(value=0.)
    localdf['replace_locid'] = (localdf[locid_var].isnull()
                                & (localdf[lon_var] != 0.)
                                & (localdf[lat_var] != 0.))

    if (np.any(localdf['replace_locid'])):
        shape_df = geopandas.read_file('../shapefiles/taxi_zones_latlon.shp')
        shape_df.drop(['OBJECTID', "Shape_Area", "Shape_Leng", "borough", "zone"],
                      axis=1, inplace=True)

        try:
            local_gdf = geopandas.GeoDataFrame(
                localdf, crs={'init': 'epsg:4326'},
                geometry=[Point(xy) for xy in
                          zip(localdf[lon_var], localdf[lat_var])])

            local_gdf = geopandas.sjoin(
                local_gdf, shape_df, how='left', op='intersects')

            # one point can intersect more than one zone -- for example if on
            # the boundary between two zones. Deduplicate by taking first valid.
            local_gdf = local_gdf[~local_gdf.index.duplicated(keep='first')]

            local_gdf.LocationID.values[~local_gdf.replace_locid] = (
                (local_gdf[locid_var])[~local_gdf.replace_locid]).values

            return local_gdf.LocationID.rename(locid_var)
        except ValueError as ve:
            print(ve)
            print(ve.stacktrace())
            return df[locid_var]
    else:
        return df[locid_var]


def get_green():
    green_schema_pre_2015 = "vendor_id,pickup_datetime,dropoff_datetime,store_and_fwd_flag,rate_code_id,pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude,passenger_count,trip_distance,fare_amount,extra,mta_tax,tip_amount,tolls_amount,ehail_fee,total_amount,payment_type,trip_type,junk1,junk2"
    green_glob_pre_2015 = glob(
        os.path.join(config['taxi_raw_data_path'], 'green_tripdata_201[34]*.csv'))

    green_schema_2015_h1 = "vendor_id,pickup_datetime,dropoff_datetime,store_and_fwd_flag,rate_code_id,pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude,passenger_count,trip_distance,fare_amount,extra,mta_tax,tip_amount,tolls_amount,ehail_fee,improvement_surcharge,total_amount,payment_type,trip_type,junk1,junk2"
    green_glob_2015_h1 = glob(
        os.path.join(config['taxi_raw_data_path'], 'green_tripdata_2015-0[1-6].csv'))

    green_schema_2015_h2_2016_h1 = "vendor_id,pickup_datetime,dropoff_datetime,store_and_fwd_flag,rate_code_id,pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude,passenger_count,trip_distance,fare_amount,extra,mta_tax,tip_amount,tolls_amount,ehail_fee,improvement_surcharge,total_amount,payment_type,trip_type"
    green_glob_2015_h2_2016_h1 = glob(os.path.join(config['taxi_raw_data_path'], 'green_tripdata_2015-0[7-9].csv')) + glob(os.path.join(
        config['taxi_raw_data_path'], 'green_tripdata_2015-1[0-2].csv')) + glob(os.path.join(config['taxi_raw_data_path'], 'green_tripdata_2016-0[1-6].csv'))

    green_schema_2016_h2 = "vendor_id,pickup_datetime,dropoff_datetime,store_and_fwd_flag,rate_code_id,pickup_location_id,dropoff_location_id,passenger_count,trip_distance,fare_amount,extra,mta_tax,tip_amount,tolls_amount,ehail_fee,improvement_surcharge,total_amount,payment_type,trip_type,junk1,junk2"
    green_glob_2016_h2 = glob(os.path.join(config['taxi_raw_data_path'], 'green_tripdata_2016-0[7-9].csv')) + glob(
        os.path.join(config['taxi_raw_data_path'], 'green_tripdata_2016-1[0-2].csv'))

    # Green
    green1 = dd.read_csv(green_glob_pre_2015, header=0,
                         na_values=["NA"],
                         parse_dates=[1, 2],
                         infer_datetime_format=True,
                         dtype=dtype_list,
                         names=green_schema_pre_2015.split(','))
    green1['dropoff_location_id'] = green1['total_amount'].copy()
    green1['dropoff_location_id'] = np.nan
    green1['pickup_location_id'] = green1['total_amount'].copy()
    green1['pickup_location_id'] = np.nan
    green1['improvement_surcharge'] = green1['total_amount'].copy()
    green1['improvement_surcharge'] = np.nan
    green1 = green1.drop(['junk1', 'junk2'], axis=1)

    green2 = dd.read_csv(green_glob_2015_h1, header=0,
                         na_values=["NA"],
                         parse_dates=[1, 2],
                         infer_datetime_format=True,
                         dtype=dtype_list,
                         names=green_schema_2015_h1.split(','))
    green2['dropoff_location_id'] = green2['total_amount'].copy()
    green2['dropoff_location_id'] = np.nan
    green2['pickup_location_id'] = green2['total_amount'].copy()
    green2['pickup_location_id'] = np.nan
    green2 = green2.drop(['junk1', 'junk2'], axis=1)

    green3 = dd.read_csv(green_glob_2015_h2_2016_h1, header=0,
                         na_values=["NA"],
                         parse_dates=[1, 2],
                         infer_datetime_format=True,
                         dtype=dtype_list,
                         names=green_schema_2015_h2_2016_h1.split(','))
    green3['dropoff_location_id'] = green3['total_amount'].copy()
    green3['dropoff_location_id'] = np.nan
    green3['pickup_location_id'] = green3['total_amount'].copy()
    green3['pickup_location_id'] = np.nan

    green4 = dd.read_csv(green_glob_2016_h2, header=0,
                         na_values=["NA"],
                         parse_dates=[1, 2],
                         infer_datetime_format=True,
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

    for field in list(green.columns):
        if field in dtype_list:
            green[field] = green[field].astype(dtype_list[field])

    green['trip_type'] = 'green'

    return green


def get_yellow():
    yellow_schema_pre_2015 = "vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,pickup_longitude,pickup_latitude,rate_code_id,store_and_fwd_flag,dropoff_longitude,dropoff_latitude,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,total_amount"
    yellow_glob_pre_2015 = glob(
        os.path.join(config['taxi_raw_data_path'], 'yellow_tripdata_201[0-4]*.csv'))

    yellow_schema_2015_2016_h1 = "vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,pickup_longitude,pickup_latitude,rate_code_id,store_and_fwd_flag,dropoff_longitude,dropoff_latitude,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount"
    yellow_glob_2015_2016_h1 = glob(os.path.join(config['taxi_raw_data_path'], 'yellow_tripdata_2015*.csv')) + glob(
        os.path.join(config['taxi_raw_data_path'], 'yellow_tripdata_2016-0[1-6].csv'))

    yellow_schema_2016_h2 = "vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,store_and_fwd_flag,pickup_location_id,dropoff_location_id,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,junk1,junk2"
    yellow_glob_2016_h2 = glob(os.path.join(config['taxi_raw_data_path'], 'yellow_tripdata_2016-0[7-9].csv')) + glob(
        os.path.join(config['taxi_raw_data_path'], 'yellow_tripdata_2016-1[0-2].csv'))

    yellow1 = dd.read_csv(yellow_glob_pre_2015, header=0,
                          na_values=["NA"],
                          parse_dates=[1, 2],
                          infer_datetime_format=True,
                          dtype=dtype_list,
                          names=yellow_schema_pre_2015.split(','))
    yellow1['dropoff_location_id'] = yellow1['total_amount'].copy()
    yellow1['dropoff_location_id'] = np.nan
    yellow1['pickup_location_id'] = yellow1['total_amount'].copy()
    yellow1['pickup_location_id'] = np.nan
    yellow1['ehail_fee'] = yellow1['total_amount'].copy()
    yellow1['ehail_fee'] = np.nan
    yellow1['improvement_surcharge'] = yellow1['total_amount'].copy()
    yellow1['improvement_surcharge'] = np.nan
    yellow1['trip_type'] = yellow1['rate_code_id'].copy()
    yellow1['trip_type'] = -999

    yellow2 = dd.read_csv(yellow_glob_2015_2016_h1, header=0,
                          na_values=["NA"],
                          parse_dates=[1, 2],
                          infer_datetime_format=True,
                          dtype=dtype_list,
                          names=yellow_schema_2015_2016_h1.split(','))
    yellow2['dropoff_location_id'] = yellow2['rate_code_id'].copy()
    yellow2['dropoff_location_id'] = np.nan
    yellow2['pickup_location_id'] = yellow2['rate_code_id'].copy()
    yellow2['pickup_location_id'] = np.nan
    yellow2['ehail_fee'] = yellow2['total_amount'].copy()
    yellow2['ehail_fee'] = np.nan
    yellow2['trip_type'] = yellow2['rate_code_id'].copy()
    yellow2['trip_type'] = -999

    yellow3 = dd.read_csv(yellow_glob_2016_h2, header=0,
                          na_values=["NA"],
                          parse_dates=[1, 2],
                          infer_datetime_format=True,
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

    for field in list(yellow.columns):
        if field in dtype_list:
            yellow[field] = yellow[field].astype(dtype_list[field])

    yellow['trip_type'] = 'yellow'

    return yellow


def get_uber():
    uber_schema_2014="pickup_datetime,pickup_latitude,pickup_longitude,junk1"
    uber_glob_2014 = glob(os.path.join(config['uber_raw_data_path'],'uber*-???14.csv'))

    uber_schema_2015="junk1,pickup_datetime,junk2,pickup_location_id"
    uber_glob_2015 = glob(os.path.join(config['uber_raw_data_path'],'uber*15.csv'))

    dtype_list = { 
        # 'dropoff_datetime': np.int64,
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

    uberdf['dropoff_datetime'] = np.datetime64("1970-01-01 00:00:00")
    #uberdf = uberdf.repartition(npartitions=20)

    uberdf['trip_type'] = 'uber'

    uberdf = uberdf[sorted(uberdf.columns)]

    return uberdf


def main(client):

    uber = get_uber()
    green = get_green()
    yellow = get_yellow()

    all_trips = uber.append(green).append(yellow)
#    all_trips['dropoff_datetime'] = all_trips.dropoff_datetime.astype(str)
#    all_trips['pickup_datetime'] = all_trips.pickup_datetime.astype(str)

#    all_trips['dropoff_datetime'] = all_trips.dropoff_datetime.str.replace('NaT', '1970-01-01 00:00:00')
#    all_trips['pickup_datetime'] = all_trips.pickup_datetime.str.replace('NaT', '1970-01-01 00:00:00')

    all_trips = all_trips[sorted(all_trips.columns)]    

    all_trips['dropoff_location_id'] = all_trips.map_partitions(
        assign_taxi_zones, "dropoff_longitude", "dropoff_latitude",
        "dropoff_location_id", meta=('dropoff_location_id', np.float64))
    all_trips['pickup_location_id'] = all_trips.map_partitions(
        assign_taxi_zones, "pickup_longitude", "pickup_latitude",
        "pickup_location_id", meta=('pickup_location_id', np.float64))

    all_trips.to_parquet(
        os.path.join(config['parquet_output_path'], 'all_trips_locid.parquet'),
        compression="SNAPPY", has_nulls=True,
        object_encoding='json')

    all_trips = dd.read_parquet(
        os.path.join(config['parquet_output_path'], 'all_trips_locid.parquet'))

    all_trips = all_trips.repartition(npartitions=1000)

    all_trips.to_csv('/data3/csv/all_trips_locid-*.csv.gz', index=False,
        name_function=lambda l: '{0:04d}'.format(l),
        compression='gzip')


if __name__ == '__main__':
    client = Client()

    main(client)
