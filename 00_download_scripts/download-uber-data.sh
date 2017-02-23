#!/bin/bash

cat urls_uber.txt | xargs -n 1 wget -P ./raw_data/uber
