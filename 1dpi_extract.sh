#!/bin/bash
# ./1dpi_extract.sh 20160215 20160216 20160217 hubei
choose_date=$1
date2=$2
date3=$3
province=$4
choose_dpi_table=dpi_${province}_comm
dpi_new_table=test_dpi_${province}_ds
db_name=dpiq_db

sql1="use ${db_name};insert overwrite table ${dpi_new_table} partition(yyyymmdd='$choose_date',ds='3g',section='1') select if(length(mdn)=13,substr(mdn,3),mdn) as msisdn,start_time,end_time,nvl(input_octets,0) as input_octets,nvl(output_octets,0) as output_octets from ${choose_dpi_table}_3g where chunk='${choose_date}' and start_time>='${choose_date}000000' and start_time<='${choose_date}235959' and (length(mdn)=11 or length(mdn)=13) and mdn is not null "

sql2="use ${db_name};insert overwrite table ${dpi_new_table} partition(yyyymmdd='$choose_date',ds='3g',section='2') select if(length(mdn)=13,substr(mdn,3),mdn) as msisdn,start_time,end_time,nvl(input_octets,0) as input_octets,nvl(output_octets,0) as output_octets from ${choose_dpi_table}_3g where chunk='${date2}' and start_time>='${choose_date}000000' and start_time<='${choose_date}235959' and (length(mdn)=11 or length(mdn)=13) and mdn is not null "

sql3="use ${db_name};insert overwrite table ${dpi_new_table} partition(yyyymmdd='$choose_date',ds='3g',section='3') select if(length(mdn)=13,substr(mdn,3),mdn) as msisdn,start_time,end_time,nvl(input_octets,0) as input_octets,nvl(output_octets,0) as output_octets from ${choose_dpi_table}_3g where chunk='${date3}' and start_time>='${choose_date}000000' and start_time<='${choose_date}235959' and (length(mdn)=11 or length(mdn)=13) and mdn is not null "

#hive -e "$sql1" 

#hive -e "$sql2" 

hive -e "$sql1;$sql2;$sql3;" 


sql4="use ${db_name};insert overwrite table ${dpi_new_table} partition(yyyymmdd='$choose_date',ds='4g',section='1') select if(length(msisdn)=13,substr(msisdn,3),msisdn) as msisdn,start_time,end_time,nvl(input_octets,0) as input_octets,nvl(output_octets,0) as output_octets from ${choose_dpi_table}_4g where chunk='${choose_date}' and start_time>='${choose_date}000000' and start_time<='${choose_date}235959' and (length(msisdn)=11 or length(msisdn)=13) and msisdn is not null"

sql5="use ${db_name};insert overwrite table ${dpi_new_table} partition(yyyymmdd='$choose_date',ds='4g',section='2') select if(length(msisdn)=13,substr(msisdn,3),msisdn) as msisdn,start_time,end_time,nvl(input_octets,0) as input_octets,nvl(output_octets,0) as output_octets from ${choose_dpi_table}_4g where chunk>='${date2}' and start_time>='${choose_date}000000' and start_time<='${choose_date}235959' and (length(msisdn)=11 or length(msisdn)=13) and msisdn is not null"

sql6="use ${db_name};insert overwrite table ${dpi_new_table} partition(yyyymmdd='$choose_date',ds='4g',section='3') select if(length(msisdn)=13,substr(msisdn,3),msisdn) as msisdn,start_time,end_time,nvl(input_octets,0) as input_octets,nvl(output_octets,0) as output_octets from ${choose_dpi_table}_4g where chunk='${date3}' and start_time>='${choose_date}000000' and start_time<='${choose_date}235959' and (length(msisdn)=11 or length(msisdn)=13) and msisdn is not null"

hive -e "$sql4;$sql5;$sql6;" 

#hive -e "$sql5" 

#hive -e "$sql6" 
