#!/usr/bin/env python
"""
Fetch subway data, store it on locally on disk.
http://web.mta.info/developers/turnstile.html

This file is copyright: Ravi Shekhar 2016
and licensed under the MIT License 
https://opensource.org/licenses/MIT
"""
from __future__ import print_function, division

import six
import six.moves.urllib as urllib
import re
import os
import os.path

from datetime import datetime
from bs4 import BeautifulSoup


def download_subway_data():
    PAGE_URL = six.u("http://web.mta.info/developers/turnstile.html")
    DATA_URL = six.u("http://web.mta.info/developers/")

    f = urllib.request.urlopen(PAGE_URL)
    content = f.read()
    parser = BeautifulSoup(content, 'html.parser')
    all_links = parser.find_all('a')

    # select all days that end in day with a 4 digit year starting with '20'
    turnstile_links = [(link.text, DATA_URL + link['href'])
                       for link in all_links
                       if re.match(u'.*day.*20..', link.text)]

    # this could fail with an exception under certain cases (no permissions,
    # path is file, etc.)
    if os.path.isdir('./raw_data/subway'):
        pass
    else:
        os.makedirs('./raw_data/subway')

    for datestr, url_file in turnstile_links:
        date = datetime.strptime(datestr, "%A, %B %d, %Y")
        new_filename = './raw_data/subway/{0}'.format(os.path.split(url_file)[-1])
        if not os.path.isfile(new_filename):
            print("Downloading {0}".format(date))
            urllib.request.urlretrieve(url_file, new_filename)
        else:
            print("File for {0} exists. Skipping.".format(date))

    print('Downloading complete.')

if __name__ == '__main__':
    download_subway_data()
