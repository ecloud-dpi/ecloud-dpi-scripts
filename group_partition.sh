#!/bin/bash
#用来将合并后的DDR数据进行分区part
id=$1
date=$2

./3ddr_combine_partition.sh ${id} ${date} 3g

./3ddr_combine_partition.sh ${id} ${date} 4g


