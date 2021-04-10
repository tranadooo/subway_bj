#!/bin/sh
basepath=$(cd `dirname $0`; pwd)
cd ${basepath}
cd ../data

data_dir="output output_sx output_ho"

for dir in $data_dir;do

if [ -d $dir ];then
echo $dir
cd $dir
rm -rf  *.chk
for f in `ls *.dat`;do 
olddate=`echo ${f:0:8}`
oldidate=`date -d "$olddate +1days" +'%Y%m%d'`
today=`date +'%Y%m%d'`
tomorrow=`date -d "$today +1days" +'%Y%m%d'`
echo "change $f filedate and inner date"
sed -i  "s/$oldidate/$tomorrow/g" $f
mv $f $today${f:8}
done
cd ..
fi

done
