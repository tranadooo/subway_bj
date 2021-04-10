#!/usr/bin/python3
#coding=UTF-8

import pymysql
import numpy as np
import pandas as pd

import sys
sys.path.append(".")
from common import *

def station_bucket(forward_on_station_name_list,forward_down_station_name_list,back_on_station_name_list,back_down_station_name_list):
    list = []
    residence_station_name_list = []
    residence_station_name_list.extend(forward_on_station_name_list)
    residence_station_name_list.extend(back_down_station_name_list)
    dataframe_residence = pd.DataFrame(np.array(residence_station_name_list), columns=["station_name"])
    s_residence = dataframe_residence.groupby(["station_name"])["station_name"].count()
    residence_key = s_residence.idxmax()
    residence_value = s_residence[residence_key]
    occupation_station_name_list = []
    occupation_station_name_list.extend(forward_down_station_name_list)
    occupation_station_name_list.extend(back_on_station_name_list)
    dataframe_occupation = pd.DataFrame(np.array(occupation_station_name_list), columns=["station_name"])
    s_occupation = dataframe_occupation.groupby(["station_name"])["station_name"].count()
    occupation_key = s_occupation.idxmax()
    occupation_value = s_occupation[occupation_key]
    list.append(residence_key)
    list.append(occupation_key)
    list.append(residence_value+occupation_value)
    return list

def line_bucket(forward_line_list,back_line_list):
    list = []
    forward_line_key = ''
    forward_line_value = 0
    back_line_key = ''
    back_line_value = 0
    if len(forward_line_list)>0:
        dataframe_forward_line = pd.DataFrame(np.array(forward_line_list), columns=["line"])
        s_forward_line = dataframe_forward_line.groupby(["line"])["line"].count()
        forward_line_key = s_forward_line.idxmax()
        forward_line_value = s_forward_line[forward_line_key]
    if len(back_line_list) > 0:
        dataframe_back_line = pd.DataFrame(np.array(back_line_list), columns=["line"])
        s_back_line = dataframe_back_line.groupby(["line"])["line"].count()
        back_line_key = s_back_line.idxmax()
        back_line_value = s_back_line[back_line_key]
    list.append(forward_line_key)
    list.append(back_line_key)
    list.append(forward_line_value+back_line_value)
    return list

def line_bucket_ex(residence_station_name,occupation_station_name,forward_line,back_line,forward_on_station_name_list, forward_down_station_name_list, back_on_station_name_list,back_down_station_name_list,forward_line_list,back_line_list):
    num = 0
    forward_index = 0
    for forward_e in forward_line_list:
        if (forward_e != forward_line) and (forward_on_station_name_list[forward_index] == residence_station_name) and (forward_down_station_name_list[forward_index] == occupation_station_name):
            num = num + 1
    back_index = 0
    for back_e in back_line_list:
        if (back_e != back_line) and (back_on_station_name_list[back_index] == occupation_station_name) and (back_down_station_name_list[back_index] == residence_station_name):
            num = num + 1
    return num

def time_bucket(forward_time_list,back_time_list):
    list = []
    forward_time_key = ''
    forward_time_value = 0
    back_time_key = ''
    back_time_value = 0
    if len(forward_time_list)>0:
        dataframe_forward_time = pd.DataFrame(np.array(forward_time_list), columns=["time"])
        s_forward_time = dataframe_forward_time.groupby(["time"])["time"].count()
        forward_time_key = s_forward_time.idxmax()
        forward_time_value = s_forward_time[forward_time_key]
    if len(back_time_list) > 0:
        dataframe_back_time = pd.DataFrame(np.array(back_time_list), columns=["time"])
        s_back_time = dataframe_back_time.groupby(["time"])["time"].count()
        back_time_key = s_back_time.idxmax()
        back_time_value = s_back_time[back_time_key]
    list.append(forward_time_key)
    list.append(back_time_key)
    list.append(forward_time_value+back_time_value)
    return list

def transfer_bucket(forward_transfer_list,back_transfer_list):
    list = []
    forward_transfer_key = ''
    forward_transfer_value = 0
    back_transfer_key = ''
    back_transfer_value = 0
    if len(forward_transfer_list)>0:
        dataframe_forward_transfer = pd.DataFrame(np.array(forward_transfer_list), columns=["transfer"])
        s_forward_transfer = dataframe_forward_transfer.groupby(["transfer"])["transfer"].count()
        forward_transfer_key = s_forward_transfer.idxmax()
        forward_transfer_value = s_forward_transfer[forward_transfer_key]
    if len(back_transfer_list)>0:
        dataframe_back_transfer = pd.DataFrame(np.array(back_transfer_list), columns=["transfer"])
        s_back_transfer = dataframe_back_transfer.groupby(["transfer"])["transfer"].count()
        back_transfer_key = s_back_transfer.idxmax()
        back_transfer_value = s_back_transfer[back_transfer_key]
    list.append(forward_transfer_key)
    list.append(back_transfer_key)
    list.append(forward_transfer_value + back_transfer_value)
    return list


def createCommuterLawData():
    cur.execute("SELECT * FROM commuter_trip_chain t")
    results = cur.fetchall()
    results=pd.DataFrame(list(results),columns=[i[0] for i in cur.description])
    
    rownum = 0
    totalnum = 0
    cur.execute("TRUNCATE TABLE commuter_law ")
    sql_insert = "insert into commuter_law (CARD_ID,RESIDENCE_STATION_NAME,OCCUPATION_STATION_NAME,FORWARD_LINE,BACK_LINE,\
                                FORWARD_ON_TIME_BUCKET,BACK_ON_TIME_BUCKET,FORWARD_TRANSFER_STATION,BACK_TRANSFER_STATION,\
                                LAW_STATION_NUM,LAW_LINE_NUM,LAW_TIME_NUM,LAW_TRANSFER_NUM,DAY_NUM) \
                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    info_dict={}
    for card_id in set(results["CARD_ID"]):
        info_dict[card_id]={}
        info_dict[card_id]["FORWARD_ON_STATION_NAME_LIST"]=[]
        info_dict[card_id]["FORWARD_DOWN_STATION_NAME_LIST"]=[]
        info_dict[card_id]["BACK_ON_STATION_NAME_LIST"]=[]
        info_dict[card_id]["BACK_DOWN_STATION_NAME_LIST"]=[]
        info_dict[card_id]["FORWARD_LINE_LIST"]=[]
        info_dict[card_id]["FORWARD_ON_TIME_BUCKET_LIST"]=[]
        info_dict[card_id]["FORWARD_TRANSFER_STATION_LIST"]=[]
        info_dict[card_id]["BACK_LINE_LIST"]=[]
        info_dict[card_id]["BACK_ON_TIME_BUCKET_LIST"]=[]
        info_dict[card_id]["BACK_TRANSFER_STATION_LIST"]=[] 
        info_dict[card_id]["DAY_NUM"]=0
    for index,row in results.iterrows():
        card_id=row["CARD_ID"]
        if row["FORWARD_ON_STATION_NAME"] != '':
            info_dict[card_id]["FORWARD_ON_STATION_NAME_LIST"].append(row["FORWARD_ON_STATION_NAME"])
        if row["FORWARD_DOWN_STATION_NAME"] != '':
            info_dict[card_id]["FORWARD_DOWN_STATION_NAME_LIST"].append(row["FORWARD_DOWN_STATION_NAME"])
        if row["BACK_ON_STATION_NAME"] != '':
            info_dict[card_id]["BACK_ON_STATION_NAME_LIST"].append(row["BACK_ON_STATION_NAME"])
        if row["BACK_DOWN_STATION_NAME"] != '':
            info_dict[card_id]["BACK_DOWN_STATION_NAME_LIST"].append(row["BACK_DOWN_STATION_NAME"])
        if row["FORWARD_LINE"] != '':
            info_dict[card_id]["FORWARD_LINE_LIST"].append(row["FORWARD_LINE"])
        if row["FORWARD_ON_TIME_BUCKET"] != '':
            info_dict[card_id]["FORWARD_ON_TIME_BUCKET_LIST"].append(row["FORWARD_ON_TIME_BUCKET"])
        if row["FORWARD_TRANSFER_STATION"] != '':
            info_dict[card_id]["FORWARD_TRANSFER_STATION_LIST"].append(row["FORWARD_TRANSFER_STATION"])
        if row["BACK_LINE"] != '':
            info_dict[card_id]["BACK_LINE_LIST"].append(row["BACK_LINE"])
        if row["BACK_ON_TIME_BUCKET"] != '':
            info_dict[card_id]["BACK_ON_TIME_BUCKET_LIST"].append(row["BACK_ON_TIME_BUCKET"])
        if row["BACK_TRANSFER_STATION"] != '':
            info_dict[card_id]["BACK_TRANSFER_STATION_LIST"].append(row["BACK_TRANSFER_STATION"])
        info_dict[card_id]["DAY_NUM"]=info_dict[card_id]["DAY_NUM"]+1


    commuter_law_array=[]
    for cardid,cardinfo in info_dict.items():
        commuter_law=[]
        forward_on_station_name_list=cardinfo["FORWARD_ON_STATION_NAME_LIST"] 
        forward_down_station_name_list=cardinfo["FORWARD_DOWN_STATION_NAME_LIST"] 
        back_on_station_name_list=cardinfo["BACK_ON_STATION_NAME_LIST"] 
        back_down_station_name_list=cardinfo["BACK_DOWN_STATION_NAME_LIST"]
        forward_line_list=cardinfo["FORWARD_LINE_LIST"]
        back_line_list=cardinfo["BACK_LINE_LIST"]
        forward_time_list=cardinfo["FORWARD_ON_TIME_BUCKET_LIST"]
        back_time_list=cardinfo["BACK_ON_TIME_BUCKET_LIST"]
        forward_transfer_list=cardinfo["FORWARD_TRANSFER_STATION_LIST"]
        back_transfer_list=cardinfo["BACK_TRANSFER_STATION_LIST"]
        day_num = cardinfo["DAY_NUM"]
        res_station = station_bucket(forward_on_station_name_list, forward_down_station_name_list, back_on_station_name_list,back_down_station_name_list)
        residence_station_name = res_station[0]
        occupation_station_name = res_station[1]
        station_num = res_station[2]
        res_line = line_bucket(forward_line_list,back_line_list)
        forward_line = res_line[0]
        back_line = res_line[1]
        line_num = res_line[2]
        line_ex_num = line_bucket_ex(residence_station_name,occupation_station_name,forward_line,back_line,forward_on_station_name_list, forward_down_station_name_list, back_on_station_name_list,back_down_station_name_list,forward_line_list,back_line_list)
        if line_ex_num>0:
            line_num = line_num + line_ex_num
        res_time = time_bucket(forward_time_list,back_time_list)
        forward_time = res_time[0]
        back_time = res_time[1]
        time_num = res_time[2]
        res_transfer = time_bucket(forward_transfer_list,back_transfer_list)
        forward_transfer = res_transfer[0]
        back_transfer = res_transfer[1]
        transfer_num = res_transfer[2]
        commuter_law.append(cardid)
        commuter_law.append(residence_station_name)
        commuter_law.append(occupation_station_name)
        commuter_law.append(forward_line)
        commuter_law.append(back_line)
        commuter_law.append(forward_time)
        commuter_law.append(back_time)
        commuter_law.append(forward_transfer)
        commuter_law.append(back_transfer)
        commuter_law.append(int(station_num))
        commuter_law.append(int(line_num))
        commuter_law.append(int(time_num))
        commuter_law.append(int(transfer_num))
        commuter_law.append(int(day_num))
        #print(commuter_law)
        commuter_law_array.append(commuter_law)
        rownum = rownum + 1
        if (rownum == batch):
            cur.executemany(sql_insert, commuter_law_array)
            totalnum = totalnum + rownum
            print(totalnum)
            rownum = 0
            commuter_law_array = []
    if len(commuter_law_array)!=0:
        cur.executemany(sql_insert, commuter_law_array)
        totalnum = totalnum + len(commuter_law_array)
        print(totalnum)
        cur.executemany(sql_insert,commuter_law_array)
    cur.close()
    connection.close()
    
    
if __name__=="__main__":
    print(get_time(),"################Starting Analysis Law####################")
    batch = 1000
    connection,cur = connect1()
    createCommuterLawData()
    print(get_time(),"################Complete Analysis Law####################")
