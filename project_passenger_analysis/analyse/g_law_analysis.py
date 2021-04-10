#!/usr/bin/python3
#coding=UTF-8

import numpy as np
import random
import pandas as pd

import sys
sys.path.append(".")
from common import *

def law_analysis(character,top_k):
    sql_show =  "SELECT a.cluster,a.%s,a.num,concat(truncate(a.num/b.total_num*100,2),\"%s\") FROM ( \
                          SELECT a.cluster,a.%s,a.num FROM  \
                          (SELECT l.CLUSTER,l.%s,COUNT(*) num FROM commuter_law l GROUP BY l.CLUSTER,l.%s) a  \
                          WHERE (SELECT COUNT(*)   FROM (\
                          SELECT l.CLUSTER,l.%s,COUNT(*) num FROM commuter_law l GROUP BY l.CLUSTER,l.%s) b   \
                          WHERE cluster=a.cluster AND num>a.num  )<%s   ) a \
                          LEFT JOIN (SELECT cluster,COUNT(*) total_num FROM commuter_law b GROUP BY cluster) b \
                          ON a.cluster=b.cluster ORDER BY a.cluster,a.num desc "   %(character,"%",character,character,character,character,character,top_k)
    connection,cur = connect()
    cur.execute(sql_show)
    results = cur.fetchall()
    result = results[0]
    key_list = [i[0] for i in cur.description]
    data_list = []
    for result_data in results:
        data_l = []
        for key in key_list:
            data_l.append(result_data[key])
        data_list.append(data_l)
    key_list_CN = ["乘客类别",character_dict[character],"数量","同类比例"]
    data_frame = pd.DataFrame(data_list,columns=key_list_CN)
    cur.close()
    connection.close()
    return data_frame

character_dict={"RESIDENCE_STATION_NAME":"最频住站","OCCUPATION_STATION_NAME":"最频职站","FORWARD_LINE":"最频去程线路过程",
                            "FORWARD_ON_TIME_BUCKET":"最频去程时段","FORWARD_TRANSFER_STATION":"最频去程换乘过程","BACK_LINE":"最频回程线路过程",
                            "BACK_ON_TIME_BUCKET":"最频回程时段","BACK_TRANSFER_STATION":"最频回程换乘过程"}
#character_dict={"RESIDENCE_STATION_NAME":"最频住站"}

if __name__ == "__main__":
    index=1
    top_k=3
    for i in character_dict:
        df=law_analysis(i,top_k)
        df.to_excel(base_dir+'/analyse/result/通勤指标-%s-统计分析1.xlsx'% \
                    (character_dict[i])  , header=True)
