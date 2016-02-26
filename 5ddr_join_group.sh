#!/bin/bash
#实例：./5ddr_join_group.sh ddr_25_join_grouped  ddr_25_join 20150122 3g 0_100k
ddr_join_group_table=$1
ddr_join_table=$2

yyyymmdd=$3
ds=$4
part=$5

hive -e"

use dpiq_db;
set hive.auto.convert.join.noconditionaltask = true;
set hive.auto.convert.join.noconditionaltask.size = 10000;

INSERT OVERWRITE TABLE  ${ddr_join_group_table} PARTITION (yyyymmdd='$yyyymmdd',ds='$ds',part='$part')
select
msisdn,start_time,end_time,
ddr_in,ddr_out,nvl(ddr_in,0)+nvl(ddr_out,0),
sum(nvl(dpi_in,0)) as dpi_in,sum(nvl(dpi_out,0)) as dpi_out ,sum(nvl(dpi_in,0))+sum(nvl(dpi_out,0)),
(sum(nvl(dpi_in,0))+sum(nvl(dpi_out,0)))/(nvl(ddr_in,0)+nvl(ddr_out,0)),count(*) as dpi_count
from $ddr_join_table
where 
yyyymmdd='$yyyymmdd' and ds='$ds' and part='$part' group by msisdn,ddr_in,ddr_out,start_time,end_time;"
