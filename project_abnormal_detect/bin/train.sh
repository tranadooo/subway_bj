#!/bin/sh
basepath=$(cd `dirname $0`; pwd)
cd ${basepath}
python3 -u train.py >>../logs/train.log 2>&1 &
exit
