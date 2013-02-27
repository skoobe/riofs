#!/bin/bash

#Create 1000 files
# usage: ./create_1000_files.sh mountpoint

for i in $(seq 1 1000)
do
    echo $i > $1/$i
done
