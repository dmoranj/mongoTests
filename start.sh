#!/bin/bash

PROCESSES=5

rm results.log

echo "Starting tests " > results.log 

for i in $(seq $PROCESSES)
do
  echo "Creating new Agent"
  nohup node testWrites.js parallel &>> results.log&
done
