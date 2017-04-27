#!/usr/bin/env python

import multiprocessing
import os
import os.path
import sys

import psycopg2


errlog = lambda l: sys.stderr.write('{}\n'.format(l))


def check_lockfile():
    "Exits if there is a lockfile present."

    if os.path.isfile(os.path.expanduser('~/.spatial_update_lock')):
        errlog('File ~/.spatial_update_lock exists. Exiting. Delete to retry.')
        sys.exit(99)
    try:
        fh = open(os.path.expanduser('~/.spatial_update_lock'), 'w')
        fh.write(str(os.getpid()))
        fh.close()
    except:
        errlog("Could not lock ~/.spatial_update_lock ... Exiting")
        sys.exit(95)


def delete_lockfile():
    "Deletes the lockfile at end of execution."

    try:
        os.remove(os.path.expanduser('~/.spatial_update_lock'))
    except Exception as e:
        errlog("Could not remove ~/.spatial_update_lock")
        raise e


def alter_tables_add_cols(conn):
    "Create columns in taxi_ingest and bike_ingest if needed."

    cur = conn.cursor()

    cur.execute("""
    ALTER TABLE bike_ingest ADD COLUMN IF NOT EXISTS start_ct_id INTEGER;
    ALTER TABLE bike_ingest ADD COLUMN IF NOT EXISTS end_ct_id INTEGER;
    ALTER TABLE bike_ingest ADD COLUMN IF NOT EXISTS start_taxizone_id INTEGER;
    ALTER TABLE bike_ingest ADD COLUMN IF NOT EXISTS end_taxizone_id INTEGER;
    ALTER TABLE bike_ingest ADD COLUMN IF NOT EXISTS inprogress BOOLEAN;

    ALTER TABLE taxi_ingest ADD COLUMN IF NOT EXISTS pickup_ct_id INTEGER;
    ALTER TABLE taxi_ingest ADD COLUMN IF NOT EXISTS dropoff_ct_id INTEGER;
    ALTER TABLE taxi_ingest ADD COLUMN IF NOT EXISTS inprogress BOOLEAN;

    """)
    conn.commit()



def create_views(conn):
    """Create views of taxi_ingest and bike_ingest where locations can be 
    assigned but have not yet been assigned."""

    cur = conn.cursor()

    cur.execute("""
        CREATE OR REPLACE VIEW unlocated_taxi AS 
            SELECT trip_id, 
                dropoff_latitude, dropoff_longitude, 
                pickup_latitude, pickup_longitude
            FROM taxi_ingest
            WHERE abs(dropoff_latitude-40.75) < 1.0 AND 
                  abs(dropoff_longitude + 73.95) < 1.0 AND
                  abs(pickup_latitude-40.75) < 1.0 AND 
                  abs(pickup_longitude + 73.95) < 1.0 AND
                  inprogress IS NULL AND
                  (dropoff_location_id IS NULL OR        
                   dropoff_ct_id IS NULL OR
                   pickup_location_id IS NULL OR        
                   pickup_ct_id IS NULL) ;
                  """)

    cur.execute("""
        CREATE OR REPLACE VIEW unlocated_bike_starts AS 
            SELECT biketrip_id, 
                start_station_latitude, start_station_longitude, 
                end_station_latitude, end_station_longitude
            FROM bike_ingest
            WHERE abs(start_station_latitude-40.75) < 1.0 AND 
                  abs(start_station_longitude + 73.95) < 1.0 AND
                  (start_ct_id IS NULL OR        
                   start_taxizone_id IS NULL) AND
                  inprogress IS NULL;
                  """)

    cur.execute("""
        CREATE OR REPLACE VIEW unlocated_bike_ends AS 
            SELECT biketrip_id, 
            FROM bike_ingest
            WHERE abs(end_station_latitude-40.75) < 1.0 AND 
                  abs(end_station_longitude + 73.95) < 1.0 AND
                  (end_ct_id IS NULL OR        
                   end_taxizone_id IS NULL) AND
                  inprogress IS NULL;
                  """)

    conn.commit()

def spatial_merge_bike(modBase=1, modVal=0):
    "Classify unlocated bike_ingest points."

    import psycopg2
    conn = psycopg2.connect(
        open(os.path.expanduser('~/.sqlconninfo')).read()
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE_TABLE bike_starts_{modBase}_{modVal} AS
        SELECT 
            trip_id, 
        FROM unlocated_bike_starts
        WHERE MOD(trip_id, {modBase})={modVal};
    """)

    conn.commit()
    conn.close()


def main():
    conn = psycopg2.connect(
        open(os.path.expanduser('~/.sqlconninfo')).read()
    )
    cur = conn.cursor()

    check_lockfile()

    alter_tables_add_cols(conn)
    create_views(conn)
    conn.close()

    NCPUS = 4

    pool = multiprocessing.Pool(NCPUS)


    delete_lockfile()


if __name__ == '__main__':
    main()
