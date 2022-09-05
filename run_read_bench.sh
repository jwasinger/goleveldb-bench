#! /usr/bin/env bash

# run pebble
rm -rf testdb-*
mkdir -p datasets/mymachine-1gb
mkdir -p read-charts

echo "pebble"

./cmd/pebble-readbench/pebble-readbench -size 1gb -logdir datasets/mymachine-1gb -test $1
./cmd/ldb-benchplot/ldb-benchplot -out ./read-charts/$1-pebble.svg datasets/mymachine-1gb/$1.json

mv datasets/mymachine-1gb/$1.json datasets/mymachine-1gb/$1-pebble.json

echo "ldb"

# run ldb
rm -rf testdb-*

./cmd/ldb-readbench/ldb-readbench -size 1gb -logdir datasets/mymachine-1gb -test $1
./cmd/ldb-benchplot/ldb-benchplot -out ./read-charts/$1-ldb.svg datasets/mymachine-1gb/$1.json

mv datasets/mymachine-1gb/$1.json datasets/mymachine-1gb/$1-ldb.json
