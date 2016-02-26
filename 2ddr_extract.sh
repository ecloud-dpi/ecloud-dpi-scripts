choose_date=$1
choose_date2=$[$1 + 2]
choose_ddr_table=$2
ddr_new_table=$3


hive -e "use dpiq_db;insert overwrite table ${ddr_new_table} partition(yyyymmdd='$choose_date',ds='3g') select if(length(msisdn=13),substr(msisdn,3),msisdn) as msisdn, start_time,if(end_time<='0',cast(start_time + call_duration as bigint),end_time) as end_time,nvl(recv_bytes,0) as recv_bytes,nvl(send_bytes,0) as send_bytes from $choose_ddr_table where chunk>='${choose_date}' and chunk<='${choose_date2}' and start_time>='${choose_date}000000' and start_time<='${choose_date}235959' and nai_type!=80 and (length(msisdn)=11 or length(msisdn)=13) and msisdn is not null and (roam_type=0 or roam_type=1)"

hive -e "use dpiq_db;insert overwrite table ${ddr_new_table} partition(yyyymmdd='$choose_date',ds='4g') select if(length(msisdn=13),substr(msisdn,3),msisdn) as  msisdn,start_time,if(end_time<='0',cast(start_time + call_duration as bigint),end_time) as end_time,nvl(recv_bytes,0) as recv_bytes,nvl(send_bytes,0) as send_bytes from $choose_ddr_table where chunk>='${choose_date}' and chunk<='${choose_date2}' and start_time>='${choose_date}000000' and start_time<='${choose_date}235959' and nai_type=80 and (length(msisdn)=11 or length(msisdn)=13) and msisdn is not null"
