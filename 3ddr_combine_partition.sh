#!/bin/bash
#实例： ./*.sh 25 20160122 3g
id=$1
to_table=ddr_${id}_ds_r_partition
from_table=ddr_${id}_ds_r
date=$2
nai_type=$3

hive -e "
use dpiq_db;
insert overwrite table ${to_table} partition(yyyymmdd=${date},ds='${nai_type}',part='0_100k') select msisdn,start_time,end_time,nvl(recv_bytes,0),nvl(send_bytes,0) from ${from_table} where yyyymmdd=${date} and ds='$nai_type' and (recv_bytes+send_bytes)<100*1024;

insert overwrite table ${to_table} partition(yyyymmdd=${date},ds='${nai_type}',part='100_200k') select msisdn,start_time,end_time,nvl(recv_bytes,0),nvl(send_bytes,0) from ${from_table} where yyyymmdd=${date} and ds='$nai_type' and (recv_bytes+send_bytes)<200*1024 and (recv_bytes+send_bytes)>=100*1024;

insert overwrite table ${to_table} partition(yyyymmdd=${date},ds='${nai_type}',part='200_500k') select msisdn,start_time,end_time,nvl(recv_bytes,0),nvl(send_bytes,0) from ${from_table} where yyyymmdd=${date} and ds='$nai_type' and (recv_bytes+send_bytes)>=200*1024 and (recv_bytes+send_bytes)<500*1024;

insert overwrite table ${to_table} partition(yyyymmdd=${date},ds='${nai_type}',part='500k') select msisdn,start_time,end_time,nvl(recv_bytes,0),nvl(send_bytes,0) from ${from_table} where yyyymmdd=${date} and ds='$nai_type' and (recv_bytes+send_bytes)>=500*1024;"

