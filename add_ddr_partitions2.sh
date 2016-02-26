#!/bin/bash
#使用说明：执行 ./group_add_partitions.sh jiangsu 3g 20160215 dpi_jiangsu_comm_3g
#即可搜索/data/hjpt/edm/cdr/mbl/itf_billing_mbl_data//jiangsu/20160215/目录下的小时的值（00,01,02...），然后
#生成添加分区的SQL语句，最后启动hive批量执行SQL语句
#参数说明：
#province=省份；chunk=添加分区的日期；tablename=hive中建立的省份对应的外表
province=$1
chunk=$2
tablename="ddr_${province}"
db="dpiq_db"

#随机生成文件，保证多人操作的时候不会发生冲突
random_var=${RANDOM}
filename="/tmp/ddr_${random_var}.txt"
sql_file="/tmp/ddr_${random_var}.sql"

hadoop_command="hadoop fs -ls /data/hjpt/edm/ilpf/comm_new/itf_${ds}dpi_mbl/${province}/${chunk}"
$hadoop_command | awk -F ' ' '{print $8}' |awk -F '/' '{print $10}'  >${filename}

for path in `cat $filename`
do
sql_temp="alter table $tablename add partition(chunk='${chunk}',form='${path}') location '/data/hjpt/edm/cdr/mbl/itf_billing_mbl_data/${province}/${chunk}/${path}';"
echo $sql_temp >>$sql_file
done

hql=`cat $sql_file`
echo "生成的SQL集合是：$hql"
hive -e "
use $db;
$hql;
"

#删除临时文件
rm $filename
rm $sql_file


