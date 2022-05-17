#! /usr/bin/env bash

# run pebble
rm -rf testdb-*
rm -rf datasets/mymachine-1gb
mkdir datasets/mymachine-1gb
rm -f ./charts/$1-pebble.svg

./cmd/pebble-readbench/pebble-readbench -size 1gb -logdir datasets/mymachine-1gb -test $1
./cmd/ldb-benchplot/ldb-benchplot -out ./charts/$1-pebble.svg datasets/mymachine-1gb/$1.json

# run ldb
rm -rf testdb-*
rm -rf datasets/mymachine-1gb
mkdir datasets/mymachine-1gb
rm -f ./charts/$1-ldb.svg

./cmd/ldb-readbench/ldb-readbench -size 1gb -logdir datasets/mymachine-1gb -test $1
./cmd/ldb-benchplot/ldb-benchplot -out ./charts/$1-ldb.svg datasets/mymachine-1gb/$1.json
