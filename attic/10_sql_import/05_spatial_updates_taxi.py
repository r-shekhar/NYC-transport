#!/usr/bin/env python

from __future__ import division

import psycopg2
import os, os.path
import dask
import dask.distributed
import dask.delayed
import numpy as np







def get_maxid(rows_per_job):
    db_uri = open(os.path.expanduser('~/.sqlconninfo')).read()
    conn = psycopg2.connect(db_uri)
    cur = conn.cursor()

    cur.execute('SELECT MAX(trip_id) FROM taxi_ingest_col;')
    z2 = cur.fetchall()
    N = z2[0][0]
    conn.close()

    n2 = int(np.ceil(N / rows_per_job))

    return N, n2


def spatial_merge_job(i, rows_per_job):
    import psycopg2
    db_uri = open(os.path.expanduser('~/.sqlconninfo')).read()
    conn = psycopg2.connect(db_uri)
    cur = conn.cursor()

    create_statement = """
    CREATE TABLE tmp_points_%s AS 
    SELECT
      trip_id,
      ST_SetSRID(ST_MakePoint(pickup_longitude, pickup_latitude), 4326) as pickup,
      ST_SetSRID(ST_MakePoint(dropoff_longitude, dropoff_latitude), 4326) as dropoff
    FROM taxi_ingest_col
    WHERE (trip_id > %s AND trip_id <= %s) AND 
        (abs(pickup_longitude + 73.95) < 1.0 AND
         abs(pickup_latitude - 40.75) < 1.0 ) OR 
        (abs(dropoff_longitude + 73.95) < 1.0 AND
         abs(dropoff_latitude - 40.75) < 1.0 
        )
    ORDER BY trip_id;
    """

    cur.execute(create_statement, (i, i*rows_per_job, (i+1)*rows_per_job))
    conn.commit()
    nrows = cur.execute("SELECT COUNT(*) FROM tmp_points_%s;", (i,))
    nrows = nrows[0][0]
    print(nrows)



    pass


def main():
    rows_per_job = 1000000
    Nrows, Njobs = get_maxid(rows_per_job)

    for i in range(1):
        spatial_merge_job(i, rows_per_job)




    pass


if __name__ == '__main__':
    # c = dask.distributed.Client()
    main()