# Pipeline stage 05 : Raw to DataFrame 

This stage converts the raw formats (csv / txt) to DataFrames. The goal here
is a direct transcription of the raw files without modifications of the 
original data. However, wherever a longitude and latitude are available, they
are classified using the New York City Taxi Zones definition provided in the 
shapefiles directory. 

The parsing is done in Python with Dask and Distributed on single node. The
vast majority of the CPU time required is consumed by the classification of 
coordinates into the taxi zones. For this, Geopandas is used to efficiently 
construct spatial joins. The spatial joins (calls to `assign_taxi_zones`) in 
`convert_bike_csv_to_parquet.py` and `convert_taxi_csv_to_parquet.py` take the 
vast majority of the CPU time (~90%) in this stage of the pipeline. 

- Adjust config.json to have correct input and output paths for your system.
- `python convert_bike_csv_to_parquet.py` -- Runs for 2 hours on a 4-core 
   instance, and produces 2GB of parquet format output.
- `python convert_subway_csv_to_parquet.py` -- Runs for 2 hours on a 4-core 
   instance, produces 1GB of parquet format output.
- `python convert_taxi_csv_to_parquet.py` -- Runs for about 32 hours (~1.3 
   days) on a 4-core instance. Produces about 140 GB of parquet format output, 
   about 70GB of which can be deleted afterwards.

It is worth pointing out that the parquet files produced have a timestamp
field stored as a 64-bit integer. This seems to follow an undocumented (and
possibly subject to change?) convention of storing the number of microseconds
(1.0e-6 s) since the unix epoch, 1970-01-01 00:00:00. Spark does not
automatically decode the timestamp variables encoded this way, and the values
must be decoded manually, by dividing by 1.0e6 and decoding as seconds since
the epoch.
