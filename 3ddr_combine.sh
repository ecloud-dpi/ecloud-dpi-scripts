#跑的例子: ./3ddr_combine.sh ddr_25_ds ddr_25_ds_r 20160122 3g 0_100k
from_table=$1
to_table=$2
date=$3
_ds=$4

sql="select msisdn,msisdn,start_time,end_time,nvl(recv_bytes,0),nvl(send_bytes,0) from dpiq_db.${from_table} where yyyymmdd=${date} and ds='${_ds}'"
database=dpiq_db
table=${to_table}
partition="yyyymmdd=${date},ds='${_ds}'"
partition_num=30
export YARN_CONF_DIR=/etc/hadoop/conf
export JAVA_HOME=/etc/alternatives/java_sdk_1.7.0

#export SPARK_CLASSPATH=${SPARK_CLASSPATH}:/usr/hdp/2.2.0.0-2041/hadoop/lib/native/Linux-amd64-64/libgplcompression.so
#current_dir=`pwd`
#--jars $libs \
# --driver-library-path $father_dir1/libbb/ \
#--driver-class-path $father_dir1/conf/ \
#--files $conf \
father_dir1=$(pwd)/lib
libs=`ls -l ${father_dir1}/libb/*.jar|awk '{printf "%s,",$9}'`
libs=${libs:0:${#libs}-1}

conf=`ls -l ${father_dir1}/conf/*|awk '{printf "%s,",$9}'`
conf=${conf:0:${#conf}-1}
/home/dpiq/scripts/lib/spark-1.3.0-bin-hadoop2.4/bin/spark-submit \
  --master yarn \
  --deploy-mode client \
  --num-executors 15 \
  --driver-memory 6g \
  --executor-memory 4G \
  --jars $libs \
  --files $conf \
  --class org.apache.spark.examples.sql.DddMerge2 \
  --driver-library-path $father_dir1/libbb/ \
  --driver-class-path $father_dir1/conf/ \
  cp.jar \
  "$sql" \
  $database \
  $table \
  $partition \
  $partition_num
