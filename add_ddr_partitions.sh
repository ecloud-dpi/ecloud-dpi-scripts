#!/bin/bash
#实例： ./add_ddr_partitions.sh ddr_jiangsu_ds jiangsu 20160215
tablename=$1
province=$2
chunk=$3
location="/data/hjpt/edm/cdr/mbl/itf_billing_mbl_data/"
db="dpiq_db"
hive -e "
use $db;
alter table $tablename add partition(chunk='${chunk}') location '${location}/$province/${chunk}/';
"
