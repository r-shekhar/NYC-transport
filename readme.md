# NYC-Transport Readme

This is a combined repository of all publicly available New York City transit 
datasets. 

- Taxi and Limousine Commission (TLC) Taxi trip Data
- FOIA requested Uber trip data for portions of 2013-2015
- Subway turnstile data from the Metropolitan Transit Authority (MTA)
- Citibike system data

This repository contains code to download all the data, clean it, remove 
corrupted data, and produce a set of pandas dataframes, which are written to 
Parquet format files using Dask and Fastparquet.

These Parquet format files are repartitioned on disk with PySpark, and
resulting files are queried with PySpark SQL and Dask to produce data science
results in Jupyter notebooks.

## Requirements

- Python 3.4+

- Beautiful Soup 4
- Bokeh
- Dask Distributed
- FastParquet
- Geopandas
- Jupyter
- Numba 0.29+
- Palettable
- PyArrow
- PySpark 2.0.2+
- Python-Snappy
- Scikit-Learn
- Seaborn

A [tutorial](https://r-shekhar.github.io/posts/data-science-environment.html)
on my blog shows how to set up an environment compatible with this analysis 
on Ubuntu. This tutorial has been tested locally and on Amazon EC2. 

## Steps

1. Setup your conda environment with the modules above. 

    ```bash
    conda install -c conda-forge \
        beautifulsoup4 bokeh distributed fastparquet geopandas \
        jupyter numba palettable pyarrow python-snappy  \
        scikit-learn seaborn
    conda install -c quasiben spark
    ```

2. Download the data in the `00_download_scripts` directory
    +  `./make_directories.sh` -- Alternatively you can create a `raw_data` 
       directory elsewhere and symlink it.
    +  `python download-subway-data.py` (~ 10 GB)
    +  `./download-bike-data.sh`  (~7 GB)
    +  `./download-taxi-data.sh`  (~250 GB)
    +  `./download-uber-data.sh`  (~5 GB)
    +  `./decompress.sh`

3. Convert the data to parquet format using scripts in [`05_raw_to_dataframe`](
https://github.com/r-shekhar/NYC-transport/tree/master/05_raw_to_dataframe). 
   Times given are on a 4GHz i5-3570K (4 core) with fast SSD and 16GB memory.
    + Adjust config.json to have correct input and output paths for your system
    + `python convert_bike_csv_to_parquet.py` (~2 hours)
    + `python convert_subway_to_parquet.py` (~2 hours)
    + `python convert_taxi_to_parquet.py` (~32 hours)

4. Repartition and recompress the parquet files for efficient access using 
   PySpark in [`06_repartition`](https://github.com/r-shekhar/NYC-transport/tree/master/06_repartition).
   This is especially useful for later stages, where queries are performed on
   Amazon EC2 using a distributed Spark engine using files on S3. 
