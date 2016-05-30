#!/bin/bash
while true; do
  date +%H:%M:%S
  du -hs ../data/rocksdb
  sleep 2
done
