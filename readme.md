# NYC-Transport Readme

This is a combined repository of all publicly available New York City transit 
datasets. 

- Taxi and Limousine Commission (TLC) Taxi trip Data
- FOIA requested Uber trip data for portions of 2013-2015
- Subway turnstile data from the Metropolitan Transit Authority (MTA)
- Citibike system data

This repository contains code to download all the data, clean it, remove 
corrupted data, and produce a set of pandas dataframes, which are written to 
Parquet format files.  

Code to import these Parquet format files into a PostgreSQL/PostGIS database is
also planned. 

## Requirements

- Python 3.4+
- Dask Distributed
- Bokeh
- Numba 0.29+
- FastParquet
- Python-Snappy

## Steps

1. Setup your conda environment with the modules above. 

    ```bash
    conda install -c conda-forge fastparquet numba   \
        pandas bokeh snappy python-snappy dask \
        distributed
    ```

2. Download the data in the `00_download_scripts` directory
    +  `./make_directories.sh` -- Alternatively you can create a `raw_data` 
       directory elsewhere and symlink it.
    +  `python download-subway-data.py`
    +  `./download-bike-data.sh`
    +  `./download-taxi-data.sh`
    +  `./download-uber-data.sh`
    +  `./decompress.sh`

3. TODO
