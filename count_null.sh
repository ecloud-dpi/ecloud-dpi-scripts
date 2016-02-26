#!/bin/bash
# 
choose_date=$1
choose_dpi_table=$2
choose_data2=$[$1 + 2]
db_name='dpiq_db'

sql1="use ${db_name};select count(*) from ${choose_dpi_table}_3g where chunk>='${choose_date}' and chunk<='${choose_date2}' and start_time>='${choose_date}000000' and end_time<='${choose_date}235959' and (length(mdn)=0 or mdn is null);select count(*) from ${choose_dpi_table}_3g where chunk>='${choose_date}' and start_time>='${choose_date}000000' and start_time<='${choose_date}235959' "
echo $sql1
hive -e "$sql1"

sql2="use ${db_name};select count(*) from ${choose_dpi_table}_4g where chunk>='${choose_date}' and chunk<='${choose_date2}' and start_time>='${choose_date}000000' and end_time<='${choose_date}235959' and (length(msisdn)=0 or msisdn is null);select count(*) from ${choose_dpi_table}_4g where chunk>='${choose_date}' and start_time>='${choose_date}000000' and start_time<='${choose_date}235959'"
echo $sql2
hive -e "$sql2"
