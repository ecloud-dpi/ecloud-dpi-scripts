#!/bin/bash

#实例： ./pichuli3.sh ddr_25_join_grouped ddr_25_join 20160122
to_table=$1
from_table=$2
date=$3

./5ddr_join_group.sh $to_table  $from_table $date 3g 0_100k
./5ddr_join_group.sh $to_table  $from_table $date 3g 100_200k
./5ddr_join_group.sh $to_table  $from_table $date 3g 200_500k
./5ddr_join_group.sh $to_table  $from_table $date 3g 500k

./5ddr_join_group.sh $to_table  $from_table $date 4g 0_100k
./5ddr_join_group.sh $to_table  $from_table $date 4g 100_200k
./5ddr_join_group.sh $to_table  $from_table $date 4g 200_500k
./5ddr_join_group.sh $to_table  $from_table $date 4g 500k
