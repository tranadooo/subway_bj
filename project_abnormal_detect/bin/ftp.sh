#!/bin/sh
basepath=$(cd `dirname $0`; pwd)
cd ${basepath}


#临时都用2019年训练集的计算结果，所以每天改一下文件名即可
sh change_ftp_file_date.sh >> ../logs/ftp.log
echo `pwd`
cd ../data
:<<2
starttime=`date +%Y-%m-%d\ %H:%M:%S`
echo $starttime >> ../logs/ftp.log
#check repeated file and mv to bak
for f in `ls output`;do
his=`grep "$f success uploaded" ../logs/ftp.log`
if [[ $his != '' ]];then
  echo "$f has been uploaded,exit now" >> ../logs/ftp.log
  exit -1
fi
done

for f in `ls output_sx`;do
his=`grep "$f success uploaded" ../logs/ftp.log`
if [[ $his != '' ]];then
  echo "$f has been uploaded,exit now" >> ../logs/ftp.log
  exit -1
fi
done
2

cd ../bin
#判断日期类型并进入相应输出目录，生成chk文件并一起上传
python3 -u  ftp.py   >> ../logs/ftp.log  2>&1 
 

