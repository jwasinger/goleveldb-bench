#! /usr/bin/env bash

# ./run_bench.sh nobatch
# ^ TODO: crashes ldb

./run_bench.sh nobatch-nosync
./run_bench.sh batch-100kb
./run_bench.sh batch-1mb
./run_bench.sh batch-5mb
./run_bench.sh batch-100kb-wb-512mb-cache-1gb

./run_bench.sh batch-100kb-nosync
./run_bench.sh batch-100kb-wb-512mb-cache-1gb-nosync
./run_bench.sh batch-100kb-ctable-64mb
./run_bench.sh batch-100kb-ctable-64mb-nosync
./run_bench.sh batch-100kb-ctable-64mb-wb-512mb-cache-1gb
./run_bench.sh batch-100kb-notx
./run_bench.sh batch-1mb-notx
./run_bench.sh batch-5mb-notx

./run_bench.sh concurrent
./run_bench.sh concurrent-nomerge
# ^ TODO these don't work for pebble
