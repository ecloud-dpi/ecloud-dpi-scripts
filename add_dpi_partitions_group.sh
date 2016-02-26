#!/bin/bash
province=$1
chunk=$2

./add_dpi_partitions.sh $province 3g $chunk dpi_${province}_comm_3g
./add_dpi_partitions.sh $province 4g $chunk dpi_${province}_comm_4g
