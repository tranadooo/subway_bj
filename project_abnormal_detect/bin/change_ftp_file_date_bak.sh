#!/bin/sh
basepath=$(cd `dirname $0`; pwd)
cd ${basepath}
cd ../data
:<<2
if [ -d output ];then
cd output
rm -rf  *.chk
for f in `ls *.dat`;do 
olddate=`echo ${f:0:8}`
middate=`echo ${f:0:4}-${f:4:2}-${f:6:2}`
n=`date -d "$middate +1days" +'%Y%m%d'`
tomorrow=`date -d "$middate +2days" +'%Y%m%d'`
echo "change $f filedate and inner date"
sed -i  "s/$n/$tomorrow/g" $f
mv $f $n${f:8}
done
cd ..
fi
2

if [ -d output_sx ];then
cd output_sx
rm -rf *.chk
for f in `ls *.dat`;do
olddate=`echo ${f:0:8}`
middate=`echo ${f:0:4}-${f:4:2}-${f:6:2}`
n=`date -d "$middate +1days" +'%Y%m%d'`
tomorrow=`date -d "$middate +2days" +'%Y%m%d'`
echo "change $f filedate and inner date"
sed -i  "s/$n/$tomorrow/g" $f
mv $f $n${f:8}
done
cd ..
fi


