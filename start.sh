#!/bin/bash

rm results.log

echo "Starting tests " > results.log 

for i in {1..5}
do
  echo "Creating new Agent"
  nohup node testWrites.js &>> results.log&
done
