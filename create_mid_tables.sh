#!/bin/bash
province=$1
db_name=dpiq_db

hive -e "
use ${db_name};

drop table if exists ddr_${province}_ds;
create table ddr_${province}_ds(
msisdn string,
start_time string,
end_time string,
recv_bytes string,
send_bytes string
)PARTITIONED BY(yyyymmdd string,ds string)
;

drop table if exists dpi_${province}_ds;
create table dpi_${province}_ds(
msisdn string,                                  
start_time string,                                     
end_time string,                                    
inputoctets string,                                      
outputoctets string                                   
)PARTITIONED BY(yyyymmdd string,ds string,section string)
;

drop table if exists ddr_${province}_ds_r;
create table ddr_${province}_ds_r(
msisdn string,
start_time string,                                     
end_time string,
recv_bytes string,
send_bytes string
)PARTITIONED BY(yyyymmdd string,ds string)
;

drop table if exists ddr_${province}_ds_r_partition;
create table ddr_${province}_ds_r_partition(
msisdn string,
start_time string,                                     
end_time string,
recv_bytes string,
send_bytes string
)PARTITIONED BY(yyyymmdd string,ds string,part string)
;

drop table if exists ddr_${province}_join;
create table ddr_${province}_join(
msisdn string,
start_time string,
end_time string,
dpi_time string,
ddr_in string,
ddr_out string,
dpi_in string,
dpi_out string
)PARTITIONED BY(yyyymmdd string,ds string,part string)
;


drop table if exists ddr_${province}_join_grouped;
create table ddr_${province}_join_grouped(
msisdn string,
start_time string,
end_time string,
ddr_in  string,
ddr_out string,
ddr_in_out string,
dpi_in  string,
dpi_out string,
dpi_in_out string,
ratio string,
dpi_count string
)PARTITIONED BY(yyyymmdd string,ds string,part string)
;
"

