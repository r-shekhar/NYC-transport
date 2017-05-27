## Pipeline Stage 00
### Downloading Data

You will need about 400GB of disk space to store the data you download, which
is about 270 GB, and the 

1. `./make_directories.sh` -- Alternatively you can create a `raw_data` 
       directory elsewhere and symlink it.
2. `python download-subway-data.py` (~ 10 GB)
3. `./download-bike-data.sh`  (~7 GB)
4. `./download-taxi-data.sh`  (~250 GB)
5. `./download-uber-data.sh`  (~5 GB)
6. `./decompress.sh`

The next stage in the data analysis pipeline is [`05_raw_to_dataframe`](https://github.com/r-shekhar/NYC-transport/tree/master/05_raw_to_dataframe).