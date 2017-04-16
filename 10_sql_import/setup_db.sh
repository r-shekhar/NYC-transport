#!/bin/bash

echo 'This script assumes it is running as the postgres user on ubuntu. '
echo 'It might need modifications when running on non ubuntu distributions. '


if [ "$(whoami)" != "postgres" ]; then
        echo "Script must be run as user: postgres"
        exit -1
fi

dropdb transitproject --if-exists
dropuser transituser --if-exists

psql <<EOF
create database transitproject;
create user transituser;
\c transitproject
grant all privileges on all tables in schema public to transituser;
alter role transituser with encrypted password 'yourpassword';
EOF

