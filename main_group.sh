#!/bin/bash
#需要传入2个参数：统计时间、以及省的ID，比如ddr_25,传入25即可
#举例：./main_pichuli.sh 20160120 27    
#后台跑实例：nohup ./main_pichuli.sh 20160122 25 >25.log 2>&1 &
#PS：注意中间表的命名都是按照一定规范来的，并且此脚本依赖其他5个脚本
#若中间发生错误可以单独重跑某个阶段
date=$1
id=$2

#抽取DPI数据
./1dpi_extract.sh $date dpi_${id} dpi_${id}_ds

#抽取DDR数据
./2ddr_extract.sh $date ddr_${id} ddr_${id}_ds

#合并DDR数据
./group_combine.sh ddr_${id}_ds ddr_${id}_ds_r $date

#将合并后的DDR数据按照part进行分区
./group_partition.sh ${id} ${date}

#DDR数据和DPI数据向关联
./group_relate.sh $date ddr_${id}_join ddr_${id}_ds_r_partition dpi_${id}_ds

#汇总数据
./group_collect.sh ddr_${id}_join_grouped ddr_${id}_join $date

