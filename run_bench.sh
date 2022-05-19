#! /usr/bin/env bash

# run pebble
rm -rf testdb-*
mkdir -p datasets/mymachine-1gb
mkdir -p write-charts

./cmd/pebble-writebench/pebble-writebench -size 1gb -logdir datasets/mymachine-1gb -test $1
./cmd/ldb-benchplot/ldb-benchplot -out ./write-charts/$1-pebble.svg datasets/mymachine-1gb/$1.json

# run ldb
rm -rf testdb-*
mkdir datasets/mymachine-1gb

./cmd/ldb-writebench/ldb-writebench -size 1gb -logdir datasets/mymachine-1gb -test $1
./cmd/ldb-benchplot/ldb-benchplot -out ./write-charts/$1-ldb.svg datasets/mymachine-1gb/$1.json
