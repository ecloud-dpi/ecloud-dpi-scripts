#!/bin/bash

#比如：./pichuli2.sh 20160122 ddr_25_join ddr_25_ds_r dpi_25_ds
date=$1
to_table=$2
ddr_table=$3
dpi_table=$4

echo "4ddr_join.sh $date $to_table $ddr_table $dpi_table 3g 0_100k"
./4ddr_join.sh $date $to_table $ddr_table $dpi_table 3g 0_100k
echo "4ddr_join.sh $date $to_table $ddr_table $dpi_table 3g 100_200k"
./4ddr_join.sh $date $to_table $ddr_table $dpi_table 3g 100_200k
echo "4ddr_join.sh $date $to_table $ddr_table $dpi_table 3g 200_500k"
./4ddr_join.sh $date $to_table $ddr_table $dpi_table 3g 200_500k
echo "4ddr_join.sh $date $to_table $ddr_table $dpi_table 3g 500k"
./4ddr_join.sh $date $to_table $ddr_table $dpi_table 3g 500k
echo "4ddr_join.sh $date $to_table $ddr_table $dpi_table 4g 0_100k"
./4ddr_join.sh $date $to_table $ddr_table $dpi_table 4g 0_100k
echo "4ddr_join.sh $date $to_table $ddr_table $dpi_table 4g 100_200k"
./4ddr_join.sh $date $to_table $ddr_table $dpi_table 4g 100_200k
echo "4ddr_join.sh $date $to_table $ddr_table $dpi_table 4g 200_500k"
./4ddr_join.sh $date $to_table $ddr_table $dpi_table 4g 200_500k
echo "4ddr_join.sh $date $to_table $ddr_table $dpi_table 4g 500k"
./4ddr_join.sh $date $to_table $ddr_table $dpi_table 4g 500k
