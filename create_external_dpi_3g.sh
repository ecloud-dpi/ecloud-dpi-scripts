#!/bin/bash
#example: ./create_external_dpi_3g.sh jiangsu
province=$1
location="/data/hjpt/edm/ilpf/comm/itf_3gdpi_mbl/${province}/"
tablename="dpi_${province}_comm_3g"
hive -e "
use dpiq_db;
CREATE EXTERNAL TABLE ${tablename}
(
  imsi STRING ,
  mdn STRING ,
  meid STRING ,
  nai STRING ,
  destination_ip STRING ,
  destination_port STRING ,
  source_ip STRING ,
  source_port STRING ,
  desn_ip STRING ,
  pcf_ip STRING ,
  ha_ip STRING ,
  userzone_id STRING ,
  bs_id STRING ,
  subnet STRING ,
  service_option STRING ,
  protocol_id STRING ,
  service_type STRING ,
  start_time STRING ,
  end_time STRING ,
  duration STRING ,
  input_octets STRING ,
  output_octets STRING ,
  input_packet STRING ,
  output_packet STRING ,
  session_id STRING ,
  record_close_cause STRING ,
  user_agent STRING ,
  destination_url STRING 
)
PARTITIONED BY(chunk STRING,form STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION '${location}'
;
"
