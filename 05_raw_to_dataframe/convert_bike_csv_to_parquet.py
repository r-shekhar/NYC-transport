#!/usr/bin/env python
# coding: utf-8

from dask.distributed import Client
from glob import glob
import dask.dataframe as dd
import json
import numpy as np
import pandas as pd
import os
import os.path

import geopandas
from shapely.geometry import Point


csv_schema = """trip_duration,start_time,stop_time,start_station_id,
start_station_name,start_station_latitude,start_station_longitude,
end_station_id,end_station_name,end_station_latitude,
end_station_longitude,bike_id,user_type,birth_year,gender""".split(',')
csv_schema = [x.strip() for x in csv_schema]

dtype_list = {
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

with open('config.json', 'r') as fh:
    config = json.load(fh)


# def assign_taxi_zones(df, lon_var, lat_var, locid_var):
#     """Joins DataFrame with Taxi Zones shapefile.

#     This function takes longitude values provided by `lon_var`, and latitude
#     values provided by `lat_var` in DataFrame `df`, and performs a spatial join
#     with the NYC taxi_zones shapefile. 

#     The shapefile is hard coded in, as this function makes a hard assumption of
#     latitude and longitude coordinates. It also assumes latitude=0 and 
#     longitude=0 is not a datapoint that can exist in your dataset. Which is 
#     reasonable for a dataset of New York, but bad for a global dataset.

#     Only rows where `df.lon_var`, `df.lat_var` are reasonably near New York,
#     and `df.locid_var` is set to np.nan are updated. 

#     Parameters
#     ----------
#     df : pandas.DataFrame or dask.DataFrame
#         DataFrame containing latitudes, longitudes, and location_id columns.
#     lon_var : string
#         Name of column in `df` containing longitude values. Invalid values 
#         should be np.nan.
#     lat_var : string
#         Name of column in `df` containing latitude values. Invalid values 
#         should be np.nan
#     locid_var : string
#         Name of column in `df` containing taxi_zone location ids. Rows with
#         valid, nonzero values are not overwritten. 
#     """

#     localdf = df[[lon_var, lat_var, locid_var]].copy()
#     # localdf = localdf.reset_index()
#     localdf[lon_var] = localdf[lon_var].fillna(value=0.)
#     localdf[lat_var] = localdf[lat_var].fillna(value=0.)
#     localdf['replace_locid'] = (localdf[locid_var].isnull()
#                                 & (localdf[lon_var] != 0.)
#                                 & (localdf[lat_var] != 0.))

#     if (np.any(localdf['replace_locid'])):
#         shape_df = geopandas.read_file('../shapefiles/taxi_zones_latlon.shp')
#         shape_df.drop(['OBJECTID', "Shape_Area", "Shape_Leng", "borough", "zone"],
#                       axis=1, inplace=True)

#         try:
#             local_gdf = geopandas.GeoDataFrame(
#                 localdf, crs={'init': 'epsg:4326'},
#                 geometry=[Point(xy) for xy in
#                           zip(localdf[lon_var], localdf[lat_var])])

#             local_gdf = geopandas.sjoin(
#                 local_gdf, shape_df, how='left', op='intersects')

#             # one point can intersect more than one zone -- for example if on
#             # the boundary between two zones. Deduplicate by taking first valid.
#             local_gdf = local_gdf[~local_gdf.index.duplicated(keep='first')]

#             local_gdf.LocationID.values[~local_gdf.replace_locid] = (
#                 (local_gdf[locid_var])[~local_gdf.replace_locid]).values

#             return local_gdf.LocationID.rename(locid_var).astype(np.float64)
#         except ValueError as ve:
#             print(ve)
#             print(ve.stacktrace())
#             return df[locid_var]
#     else:
#         return df[locid_var]

def assign_locations(df, lon_var, lat_var, locid_var, ctid_var):

    df = df.reset_index(drop=True)

    localdf = df[[lon_var, lat_var, locid_var]].copy()
    localdf.columns = ['lon', 'lat', 'locid']

    localdf = localdf[(localdf.locid.isnull()) 
                    & ((localdf.lon + 74.).abs() < 1.)
                    & ((localdf.lat - 40.75).abs() < 1.)]

    if localdf.shape[0] > 0:

        # perform spatial joins

        import sqlalchemy, uuid, os

        engine = sqlalchemy.create_engine(
            open(os.path.expanduser('~/.sqlconninfo')).read())
        conn = engine.connect()

        uu = uuid.uuid1().hex
        tableID = 'uu_{}'.format(uu)
        tableIDLoc = 'uuloc_{}'.format(uu)


        
        localdf.to_sql(tableID, engine, index_label='trip_id')
        conn.execute('''CREATE UNLOGGED TABLE {} AS
        SELECT
          trip_id,
          ST_SetSRID(ST_MakePoint(lon, lat), 4326) as loc
        FROM {}
        WHERE locid IS NULL
        ;
        CREATE INDEX on {} USING GIST(loc);
        '''.format(tableIDLoc, tableID, tableIDLoc))

        df1 = pd.read_sql('''SELECT t.trip_id, n.gid as census_tract_id
            FROM {} AS t, nyct2010 AS n
            WHERE ST_Within(t.loc, n.geom) ORDER BY t.trip_id;'''.format(tableIDLoc), engine)
        df2 = pd.read_sql('''SELECT t.trip_id, n.gid as taxi_zone_id
            FROM {} AS t, taxi_zones AS n
            WHERE ST_Within(t.loc, n.geom) ORDER BY t.trip_id;'''.format(tableIDLoc), engine)

        conn.execute('DROP TABLE {}; DROP TABLE {};'.format(tableIDLoc, tableID))
        conn.close()

        df1 = df1.set_index('trip_id')
        df2 = df2.set_index('trip_id')

        df1.census_tract_id = df1.census_tract_id.astype(np.float64)
        df2.taxi_zone_id = df2.taxi_zone_id.astype(np.float64)

        returnVal =  df.merge(
            df1, left_index=True, right_index=True, how='left', sort=True).merge(
            df2, left_index=True, right_index=True, how='left', sort=True)

        replace_condition = returnVal[locid_var].isnull() & returnVal['taxi_zone_id'].notnull()

        returnVal[locid_var].ix[replace_condition] = returnVal['taxi_zone_id'].ix[replace_condition]
        returnVal[ctid_var] = returnVal['census_tract_id']

        returnVal = returnVal.drop(['taxi_zone_id', 'census_tract_id'], axis=1)

        return returnVal
    else:
        # Don't have valid latitudes and longitudes, or locations have already 
        # been assigned, so do nothing
        return df


def main(client):
    df = dd.read_csv(
        sorted(
            glob(os.path.join(config["citibike_raw_data_path"],
                              '2*iti*.csv'))),
        parse_dates=[1, 2, ],
        infer_datetime_format=True,
        na_values=["\\N"],
        header=0,
        names=csv_schema,
        dtype=dtype_list
    )

    for fieldName in csv_schema:
        if fieldName in dtype_list:
            df[fieldName] = df[fieldName].astype(dtype_list[fieldName])

    df['start_taxizone_id'] = df['start_station_latitude'].copy()
    df['start_taxizone_id'] = np.nan
    df['end_taxizone_id'] = df.start_taxizone_id.copy()
    df['start_ct_id'] = df.start_taxizone_id.copy()
    df['end_ct_id'] = df.start_taxizone_id.copy()

    for x in ('start_taxizone_id', 'end_taxizone_id', 'start_ct_id', 'end_ct_id'):
        df[x] = df[x].astype(np.float64)

    df = df.map_partitions(
        assign_locations, "end_station_longitude", "end_station_latitude",
        "end_taxizone_id", "end_ct_id", meta=df
        )
    df = df.map_partitions(
        assign_locations, "start_station_longitude", "start_station_latitude",
        "start_taxizone_id", "start_ct_id", meta=df
        )

    df['start_station_name'] = df.start_station_name.str.strip('"')
    df['end_station_name'] = df.end_station_name.str.strip('"')

    df.to_parquet(
        os.path.join(config['parquet_output_path'], 'citibike.parquet'),
        compression="SNAPPY", object_encoding='json', has_nulls=True)

    # df = dd.read_parquet(os.path.join(
    #    config['parquet_output_path'], 'citibike.parquet'))

    # df['start_time'] = df.start_time.astype(str)
    # df['stop_time'] = df.stop_time.astype(str)

    # df = df.set_index('start_station_id', npartitions=30)
    # df = df.reset_index(drop=True)

    # df.to_parquet(
    #     os.path.join(config['parquet_output_path'], 'citibike_repart.parquet'),
    #     compression="SNAPPY", object_encoding='json', has_nulls=True)

    # df.to_csv(
    #    os.path.join(config["parquet_output_path"], 'csv/citibike-*.csv'), 
    #    index=False,
    #    name_function=lambda l: '{0:04d}'.format(l)
    #    )



if __name__ == '__main__':
    client = Client()
    main(client)
