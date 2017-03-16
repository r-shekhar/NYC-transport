#!/usr/bin/env python

from dask.distributed import Client
from glob import glob

import pandas as pd
from dask import delayed
import dask.bag as db
import json
import numpy as np
import os
import six
import itertools
from dateutil import parser

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
        return None

    l2 = l.split(',')
    Nfields = len(l2)

    RV = None
    try:
        if (Nfields == 11):
            # new format
            values = list(l2)

            # fields 7 and 8 contain the date and time respectively
            # combine into one field and parse
            values[6] = parser.parse("{} {}".format(*values[6:8])).isoformat()
            values.pop(7)  # folded info into item 6

            for i in range(len(datatypes)):
                if datatypes[i] == np.int64:
                    values[i] = int(values[i])
            # assert(len(values) == 10)
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
                values[6] = parser.parse("{} {}".format(*values[6:8])).isoformat()
                values.pop(7) # folded info into item 6
                for i in range(len(datatypes)):
                    if datatypes[i] == np.int64:
                        values[i] = int(values[i])
                outdata.append(tuple(values))
            RV = tuple(outdata)
        else:
            # Format not recognized
            # print so output can be looked at manually
            # print(l)
            RV = None
    except Exception as e:
        print(e)
        RV = None
        pass

    return RV

def parse_single_file(filename):
    with open(filename) as fh:
        d = filter(lambda l: not (l is None), map(parse_line, fh.readlines()), )
        d = list(itertools.chain(*d))

        df = pd.DataFrame(d, columns=columns)
        print(df.shape)
        print( '{} : {}'.format(filename, len(d)))
        return d

def main(files):

    bag = db.from_delayed([delayed(parse_single_file)(fn) for fn in files])
    df = bag.to_dataframe(columns=columns)

    df['endtime'] = df['endtime'].astype(np.datetime64)
    df['endtime'] = (df.endtime.astype(np.int64) // 1e9).astype(np.int64)

    df.cumul_entries.astype(np.int64)
    df.cumul_exits.astype(np.int64)

    # df = df.categorize()
    # fastparquet.write(os.path.join(config['parquet_output_path'], 'subway.parquet'), df,
    #                   compression='SNAPPY', object_encoding='json')

    # df = df.repartition(npartitions=50)
    df.to_parquet(os.path.join(config['parquet_output_path'], 'subway.parquet'),
                  compression="SNAPPY", object_encoding='json'
                  )


if __name__ == '__main__':
    client = Client()
    files = sorted(
            glob(os.path.join(config["subway_raw_data_path"],
                              'turnstile*.txt')))

    # parse_single_file('../00_download_scripts/raw_data/subway/turnstile_150328.txt')
    main(files)

