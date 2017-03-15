#!/usr/bin/env python

from dask.distributed import Client
from glob import glob

import dask
import dask.bag as db
import dask.dataframe as dd
import json
import numpy as np
import os
import six
import datetime
import pprint

columns = ('ca', 'unit', 'scp', 'station', 'linename', 'division',
           'endtime', 'description', 'cumul_entries', 'cumul_exits')
datatypes = (object, object, object, object, object, object,
             object, object, np.int64, np.int64)

dtype_list = {'ca':  object,
 'cumul_entries':  np.int64,
 'cumul_exits':  np.int64,
 'description':  object,
 'division':  object,
 'endtime':  object,
 'linename':  object,
 'scp':  object,
 'station':  object,
 'unit':  object}

with open('config.json', 'r') as fh:
    config = json.load(fh)


def grouper(iterable, n, fillvalue=None):
    # from the itertools recipes
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return six.itertools.zip_longest(fillvalue=fillvalue, *args)

def parse_line(l):
    l = l.strip()

    new_header_line = \
        'C/A,UNIT,SCP,STATION,LINENAME,DIVISION,DATE,TIME,DESC,ENTRIES,EXITS'

    if (len(l) == 0) or (l == new_header_line):
        return (None,)

    l2 = l.split(',')
    Nfields = len(l2)

    RV = None
    try:
        if (Nfields == 11):
            # new format
            values = list(l2)
            for i in range(len(datatypes)):
                # fields 7 and 8 contain the date and time respectively
                # replace with a python datetime
                values[6] = datetime.datetime.strftime(
                    datetime.datetime.strptime("%s %s" %
                                               (values[6], values[7]),
                                               '%m/%d/%Y %H:%M:%S'),
                    '%Y-%m-%d %H:%M:%S')
                values.pop(7) # folded info into item 6

                if datatypes[i] == 'text':
                    continue
                elif datatypes[i] == 'integer':
                    values[i] = int(values[i])
                else:
                    # it should be ok to let time and text datatypes through
                    pass

            # values.insert(0, None)  # for the primary key
            RV = (tuple(values), )

        elif (Nfields - 3) % 5 == 0:  # three constant identifiers,
                                        # multiple of 5 data points per line
            # old format
            N_datapoints = int((Nfields - 3) / 5)

            outdata = []
            for t in grouper(l2[3:], 5):  # select elements 5 at a time
                values = l2[0:3]
                values.extend(['NULL', ]*3)
                values.extend(t)
                assert(len(values) == 11)
                values[6] = datetime.datetime.strftime(
                    datetime.datetime.strptime("%s %s" %
                                               (values[6], values[7]),
                                               '%m-%d-%y %H:%M:%S'),
                    '%Y-%m-%d %H:%M:%S')
                values.pop(7) # folded info into item 6
                for i in range(len(datatypes)):
                    if datatypes[i] == 'text':
                        continue
                    elif datatypes[i] == 'integer':
                        values[i] = int(values[i])
                    else:
                        # it should be ok to let time and text datatypes
                        # through
                        pass
                # values.insert(0, None)
                outdata.append(tuple(values))
            RV = tuple(outdata)
        else:
            # Format not recognized
            # print so output can be looked at manually
            print(l)
            RV = None
    except Exception as e:
        RV = None
        pass

    return RV


def main(filelist):
    b = db.read_text(filelist).str.strip().map(parse_line)
    # b = b.concat().filter(lambda l: (l is not None) and (len(l) == 10))
    # b = b.compute(get=dask.async.get_sync)


    # b.to_textfiles(os.path.join(config['parquet_output_path'], 'junk/*.txt'))
    
    df = b.to_dataframe(columns=columns)
    # df = df.compute()
    # print(df)
    df = df.repartition(npartitions=50)
    df['cumul_exits'] = df.cumul_exits.astype(np.float64)
    df['cumul_entries'] = df.cumul_entries.astype(np.float64)
    df['endtime'] = df.endtime.astype('M8[s]')

    dd.to_parquet(os.path.join(config['parquet_output_path'], 'subway.parquet'),
        df, compression='SNAPPY', object_encoding='json')

def load_raw_into_sql(filelist):
    "Parse csv files, load data"

    # Ideally, pandas.read_csv would do this in seconds. Unfortunately this
    # data has quite a few (approximately ~5000) formatting errors and a change
    # in format mid-file. So must do it manually.

    # chains together the files into a single iterable
    with fileinput.input(files=filelist) as f:
        for l in f:
            RV = parse_line(l)

            if not (RV is None):
                insert_query = '''INSERT INTO entries (ca, unit, scp, station, 
                   linename, division, endtime, description, 
                   cumul_entries, cumul_exits) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'''
                for r in RV:
                    try:
                        pg_cur.execute(insert_query, r)
                    except ValueError as v:
                        sys.stderr.write(r)
                        sys.stderr.write(v)
                        sys.stderr.write(os.linesep)


if __name__ == '__main__':
    # client = Client()
    # load_raw_into_sql(get_raw_data_files())

    files = sorted(
            glob(os.path.join(config["subway_raw_data_path"],
                              'turnstile*.txt')))
    main(files)

