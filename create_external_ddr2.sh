#!/bin/bash
province=$1
tablename="ddr_${province}"
location="/data/hjpt/edm/cdr/mbl/itf_billing_mbl_data/${province}/"
hive -e "
use dpiq_db;
CREATE EXTERNAL TABLE ${tablename}
(
  biz_type STRING ,
  msisdn STRING ,
  imsi STRING ,
  psdn STRING ,
  apnni STRING ,
  nai_apnoi STRING ,
  nai_type STRING ,
  bsid STRING ,
  pcf STRING ,
  homeareacode STRING ,
  visitareacode STRING ,
  recv_bytes STRING ,
  send_bytes STRING ,
  roam_type STRING ,
  start_time STRING ,
  end_time STRING ,
  call_duration STRING ,
  basic_fee STRING ,
  acct_item_type_a STRING ,
  fee_add STRING ,
  acct_item_type_b STRING ,
  carried_cd STRING ,
  servid STRING ,
  billing_mode STRING ,
  event_type STRING ,
  product_id STRING ,
  esn_code STRING ,
  Imei STRING,
  prov_offer_id STRING,
  prov_ratedate STRING,
  prov_billing_cycle_id STRING,
  cdr_key STRING,
  rate_time STRING,
  rg_pid STRING,
  reserver2 STRING,
  reserver3 STRING,
  reserver4 STRING,
  reserver5 STRING
)
PARTITIONED BY(chunk STRING,form STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION '${location}'
;
"
