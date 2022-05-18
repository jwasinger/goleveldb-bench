#! /usr/bin/env bash

./run_read_bench.sh random-read
./run_read_bench.sh random-read-filter
./run_read_bench.sh random-read-bigcache
./run_read_bench.sh random-read-bigcache-filter
