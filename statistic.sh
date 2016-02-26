#!/bin/sh
#核对总量
#使用范例：./statistic_sum.sh 20160122 25
date=$1
id=$2

echo "DDR数据总条数："
hive -e "
use dpiq_db;
select yyyymmdd,ds,count(*) from  ddr_${id}_ds where yyyymmdd='$date' group by yyyymmdd,ds;
"

echo -e "\nDDR预处理后的总条数和DDR的总流量"
hive -e "
use dpiq_db;
select yyyymmdd,ds,part,(sum(nvl(recv_bytes,0))+sum(nvl(send_bytes,0)))/1024/1024/1024 ,count(*) from ddr_${id}_ds_r_partition where yyyymmdd='$date' group by yyyymmdd,ds,part order by ds;
"

echo "DPI数据流量和DPI数据总条数统计："
hive -e "
use dpiq_db;
select yyyymmdd,ds,(sum(nvl(inputoctets,0))+sum(nvl(outputoctets,0)))/1024/1024/1024  , count(*) from dpi_${id}_ds where yyyymmdd='$date' group by yyyymmdd,ds order by ds;
"
echo -e "\n————————————————————————————————————————————————"
echo "统计ddr_${id}_join_grouped中匹配0_100k,0-100k只要有数据，都认为匹配："
hive -e "
use dpiq_db;

select yyyymmdd,ds,part,count(*),sum(dpi_count),sum(ddr_in_out)/1024/1024/1024,sum(dpi_in_out)/1024/1024/1024  from ddr_${id}_join_grouped where part='0_100k' and yyyymmdd='$date' group by yyyymmdd,ds,part order by ds,part;
"

echo -e "\n统计ddr_${id}_join_grouped中匹配100_200k,有上之下分别是匹配部分，DDR流量>DPI，DDR流量<DPI:"
echo "匹配部分"
hive -e "
use dpiq_db;
select yyyymmdd,ds,part,count(*),sum(dpi_count),sum(ddr_in_out)/1024/1024/1024,sum(dpi_in_out)/1024/1024/1024  from ddr_${id}_join_grouped where   ratio>=0 and ratio<=2 and part='100_200k'  and yyyymmdd='$date'  group by yyyymmdd,ds,part order by ds,part;
"
echo "DDR流量>DPI流量："
hive -e "
use dpiq_db;
select yyyymmdd,ds,part,count(*),sum(dpi_count),sum(ddr_in_out)/1024/1024/1024,sum(dpi_in_out)/1024/1024/1024  from ddr_${id}_join_grouped where   ratio<0 and part='100_200k' and yyyymmdd='$date'  group by yyyymmdd,ds,part order by ds,part;
"
echo "DDR流量<DPI流量："
hive -e "
use dpiq_db;
select yyyymmdd,ds,part,count(*),sum(dpi_count),sum(ddr_in_out)/1024/1024/1024,sum(dpi_in_out)/1024/1024/1024  from ddr_${id}_join_grouped where   ratio>2 and part='100_200k' and yyyymmdd='$date'  group by yyyymmdd,ds,part order by ds,part;
"

echo -e "\n统计ddr_${id}_join_grouped中匹配200_500k,有上之下分别是匹配部分，DDR流量>DPI，DDR流量<DPI:"
echo "匹配部分："
hive -e "
use dpiq_db;
select yyyymmdd,ds,part,count(*),sum(dpi_count),sum(ddr_in_out)/1024/1024/1024,sum(dpi_in_out)/1024/1024/1024  from ddr_${id}_join_grouped where   ratio>=0.5 and ratio<=1.5 and part='200_500k'  and yyyymmdd='$date' group by yyyymmdd,ds,part order by ds,part;
"
echo "DDR流量>DPI流量："
hive -e "
use dpiq_db;
select yyyymmdd,ds,part,count(*),sum(dpi_count),sum(ddr_in_out)/1024/1024/1024,sum(dpi_in_out)/1024/1024/1024  from ddr_${id}_join_grouped where   ratio<0.5 and part='200_500k'  and yyyymmdd='$date' group by yyyymmdd,ds,part order by ds,part;
"
echo "DDR流量<DPI流量："
hive -e "
use dpiq_db;
select yyyymmdd,ds,part,count(*),sum(dpi_count),sum(ddr_in_out)/1024/1024/1024,sum(dpi_in_out)/1024/1024/1024  from ddr_${id}_join_grouped where   ratio>1.5 and part='200_500k' and yyyymmdd='$date'  group by yyyymmdd,ds,part order by ds,part;
"
echo -e "\n统计ddr_${id}_join_grouped中匹配500k,有上之下分别是匹配部分，DDR流量>DPI，DDR流量<DPI:"
hive -e "
use dpiq_db;
select yyyymmdd,ds,part,count(*),sum(dpi_count),sum(ddr_in_out)/1024/1024/1024,sum(dpi_in_out)/1024/1024/1024  from ddr_${id}_join_grouped where   ratio>=0.8 and ratio<=1.2 and part='500k' and yyyymmdd='$date'  group by yyyymmdd,ds,part order by ds,part;
"
echo "DDR流量>DPI流量："
hive -e "
use dpiq_db;
select yyyymmdd,ds,part,count(*),sum(dpi_count),sum(ddr_in_out)/1024/1024/1024,sum(dpi_in_out)/1024/1024/1024  from ddr_${id}_join_grouped where   ratio<0.8 and part='500k' and yyyymmdd='$date'  group by yyyymmdd,ds,part order by ds,part;
"
echo "DDR流量<DPI流量："
hive -e "
use dpiq_db;
select yyyymmdd,ds,part,count(*),sum(dpi_count),sum(ddr_in_out)/1024/1024/1024,sum(dpi_in_out)/1024/1024/1024  from ddr_${id}_join_grouped where   ratio>1.2 and part='500k' and yyyymmdd='$date' group by yyyymmdd,ds,part order by ds,part;

"
