#!/bin/bash
#example:./4g_create.sh hubei
province=$1
location="/data/hjpt/edm/ilpf/comm/itf_4gdpi_mbl/${province}/"
tablename="dpi_${province}_comm_4g"
hive -e "
use dpiq_db;
CREATE EXTERNAL TABLE ${tablename}
(
  imsi STRING ,
  msisdn STRING ,
  imei STRING ,
  apn STRING ,
  destination_ip STRING ,
  destination_port STRING ,
  source_ip STRING ,
  source_port STRING ,
  sgw_ip STRING ,
  mme_ip STRING ,
  pgw_ip STRING ,
  sai STRING ,
  tai STRING ,
  visited_plmn_id STRING ,
  rat_type STRING ,
  protocol_id STRING ,
  service_type STRING ,
  start_time STRING ,
  end_time STRING ,
  duration STRING ,
  input_octets STRING ,
  output_octets STRING ,
  input_packet STRING ,
  output_packet STRING ,
  pdn_connection_id STRING ,
  bearer_id STRING ,
  bearer_qos STRING ,
  record_close_cause STRING
)
PARTITIONED BY(chunk STRING,form STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION '${location}'
;
"
