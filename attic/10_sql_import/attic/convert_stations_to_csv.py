#!/usr/bin/env python

import pandas as pd
import json
import os.path

# This file was downloaded from 
# https://feeds.citibikenyc.com/stations/stations.json
with open('stations.json', 'r') as fh:
    j = json.load(fh)
    df = pd.DataFrame(j['stationBeanList'])

    df = df.drop(['altitude', 'availableBikes', 'availableDocks', 'city', 
        'landMark', 'lastCommunicationTime', 'location', 'postalCode', 
        'stAddress1', 'stAddress2', 'statusKey', 'testStation'], axis=1)

    pd.set_option('display.width', 1000)
    pd.set_option('display.max_rows', 1000)

    print(df)

    df.to_csv('stations-2017.04.18.csv', index=False)