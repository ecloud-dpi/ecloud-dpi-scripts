#例子：./4ddr_join.sh 20160122 ddr_25_join ddr_25_ds_r dpi_25_ds 3g 0_100k
yyyymmdd=$1
ddr_join_table=$2
ddr_table=$3
dpi_table=$4
ds=$5
part=$6

hive -e"
use dpiq_db;
set hive.auto.convert.join.noconditionaltask = true;
set hive.auto.convert.join.noconditionaltask.size = 10000;


INSERT OVERWRITE TABLE $ddr_join_table PARTITION (yyyymmdd='$yyyymmdd',ds='$ds',part='$part')
select ddr.msisdn,ddr.start_time,ddr.end_time,dpi.start_time dpi_time, 
  ddr.recv_bytes ddr_in,ddr.send_bytes ddr_out, dpi.inputoctets dpi_in,dpi.outputoctets dpi_out
from (select * from $ddr_table  where  yyyymmdd='$yyyymmdd' and ds='$ds' and part='$part') ddr, 
(select * from $dpi_table where yyyymmdd='$yyyymmdd' and ds='$ds') dpi
 where 
	ddr.msisdn=dpi.msisdn and
	dpi.start_time>=ddr.start_time and 
	dpi.start_time<=ddr.end_time;"
