#!/bin/bash
#比如： ./pichuli.sh ddr_25_ds ddr_25_ds_r 20160122
from_table=$1
to_table=$2
date=$3
#单线程处理DDR数据合并

echo "$from_table $to_table $date 3g"
./3ddr_combine.sh $from_table $to_table $date 3g

echo "$from_table $to_table $date 4g"
./3ddr_combine.sh $from_table $to_table $date 4g
