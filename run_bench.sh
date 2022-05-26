#! /usr/bin/env bash

# run pebble
rm -rf testdb-*
mkdir -p datasets/mymachine-1gb
mkdir -p write-charts

./cmd/pebble-writebench/pebble-writebench -size 1gb -logdir datasets/mymachine-1gb -test $1
./cmd/ldb-benchplot/ldb-benchplot -out ./write-charts/$1-pebble.svg datasets/mymachine-1gb/$1.json

mv datasets/mymachine-1gb/$1.json datasets/mymachine-1gb/$1-pebble.json

# run ldb
rm -rf testdb-*

./cmd/ldb-writebench/ldb-writebench -size 1gb -logdir datasets/mymachine-1gb -test $1
./cmd/ldb-benchplot/ldb-benchplot -out ./write-charts/$1-ldb.svg datasets/mymachine-1gb/$1.json

mv datasets/mymachine-1gb/$1.json datasets/mymachine-1gb/$1-ldb.json
