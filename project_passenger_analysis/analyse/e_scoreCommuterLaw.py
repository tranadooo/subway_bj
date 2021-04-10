#!/usr/bin/python3
#coding=UTF-8

import pymysql
import numpy as np

import sys
sys.path.append(".")
from common import *


def get_result_data():
    try:
        sql_sample = "select CARD_ID,LAW_STATION_NUM,LAW_LINE_NUM,LAW_TIME_NUM,DAY_NUM from commuter_law "
        cur.execute(sql_sample)
        results = cur.fetchall()
        print("总共数量＝" + str(len(results)))
        return results
    except Exception as e:
        print(e.message)
        raise e

def get_data_list(result_list,key_list):
    try:
        data_list = []
        for key in key_list:
            data_l = []
            for result_data in result_list:
                data_l.append(result_data[key])
            data_list.append(data_l)
        return data_list
    except Exception as e:
        print(e.message)
        raise e

def get_scale_list1(data_list):
    try:
        scale_list = []
        for data in data_list:
            mi = np.min(data)
            ma = np.max(data)
            data_l = []
            for v in data:
                data_l.append((v-mi)/(ma-mi))
            scale_list.append(data_l)
        return scale_list
    except Exception as e:
        print(e.message)
        raise e
def get_scale_list(data_list):
    try:
        scale_list = []
        for data in data_list:
            s = np.sum(data)
            data_l = []
            for v in data:
                data_l.append(v/s)
            scale_list.append(data_l)
        return scale_list
    except Exception as e:
        print(e.message)
        raise e

def get_feature_weight(scale_list):
    try:
        entropy_list = []

        k = -1/np.log(len(scale_list[0]))

        for data in scale_list:
            p = 0.0
            for v in data:
                if v==0:
                    p = p + 0.00001
                else:
                    logv = np.log2(v)
                    p = p + v * logv
            p = p * k
            entropy_list.append(p)
            
        #添加下面三行以修正书中直接把上面的list当作权重的错误    
        entropy_list =  [1-i for i in entropy_list]   
        entropy_sum = sum(entropy_list)
        weight_list = [i/entropy_sum for i in entropy_list]
        
        return weight_list

    except Exception as e:
        print(e.message)
        raise e

def get_data_preprocess_list(scale_list,weight_list):
    try:
        data_preprocess_list = []

        index = 0
        for data in scale_list:
            data_preprocess = []
            for v in data:
                if v==0:
                    data_preprocess.append(0.00001 * weight_list[index])
                else:
                    data_preprocess.append(v * weight_list[index])
            data_preprocess_list.append(data_preprocess)
            index = index + 1
        return data_preprocess_list

    except Exception as e:
        print(e.message)
        raise e

def get_ID():
    try:
        result_list = get_result_data()
        key_list = ["CARD_ID"]
        data_list = get_data_list(result_list,key_list)[0]
        return data_list
    except Exception as e:
        print(e.message)
        raise e

def get_data():
    try:
        result_list = get_result_data()
        key_list = ["LAW_STATION_NUM","LAW_LINE_NUM","LAW_TIME_NUM","DAY_NUM"]
        data_list = get_data_list(result_list,key_list)
        scale_list = get_scale_list(data_list)
        weight_list = get_feature_weight(scale_list)
        data_preprocess_list = get_data_preprocess_list(scale_list, weight_list)
        return data_preprocess_list
    except Exception as e:
        print(e.message)
        raise e

def prep_data_to_table(prep_table_list):
    cur.execute("Truncate table commuter_preprocessed_tmp")
    sql_insert = "insert into commuter_preprocessed_tmp  \
                            (CARD_ID,LAW_STATION_NUM_PREPED,LAW_LINE_NUM_PREPED,LAW_TIME_NUM_PREPED,DAY_NUM_PREPED) \
                         VALUES(%s,%s,%s,%s,%s)"    
    cur.executemany(sql_insert,prep_table_list)
    sql_update = "update commuter_law l  left join commuter_preprocessed_tmp p on l.card_id = p.card_id SET \
                                l.LAW_STATION_NUM_PREPED=p.LAW_STATION_NUM_PREPED, \
                                l.LAW_LINE_NUM_PREPED=p.LAW_LINE_NUM_PREPED,\
                                l.LAW_TIME_NUM_PREPED=p.LAW_TIME_NUM_PREPED,\
                                l.DAY_NUM_PREPED=p.DAY_NUM_PREPED \
                                WHERE 1=1"
    cur.execute(sql_update)
    cur.execute("COMMIT")
    return
        
def get_sample_array(data_preprocess_list):
    try:
        data_preprocess_array = np.array(data_preprocess_list)
        sample_list = data_preprocess_array.transpose()
        return sample_list
    except Exception as e:
        print(e.message)
        raise e

def get_goodandbad_sample(data_preprocess_list):
    try:
        goodandbad_list = []
        good_sample = []
        bad_sample = []
        for data in data_preprocess_list:
            good_sample.append(np.max(data))
            bad_sample.append(np.min(data))
        goodandbad_list.append(good_sample)
        goodandbad_list.append(bad_sample)
        goodandbad_array = np.array(goodandbad_list)
        return goodandbad_array
    except Exception as e:
        print(e.message)
        raise e

def get_distance(sample_array,maximum):
    try:
        distance_list = []
        good_sample = maximum[0]
        bad_sample = maximum[1]
        for sample in sample_array:
            distance = []
            dis1 = np.sqrt(np.sum(np.square(sample - good_sample)))
            dis2 = np.sqrt(np.sum(np.square(sample - bad_sample)))
            distance.append(dis1)
            distance.append(dis2)
            distance_list.append(distance)
        return distance_list
    except Exception as e:
        print(e.message)
        raise e

def get_score(distance_list):
    try:
        score_list = []
        for distance in distance_list:
            score = distance[1]/(distance[0] + distance[1])
            score_list.append(score)
        return score_list
    except Exception as e:
        print(e.message)
        raise e

        
def update_score(score_list):
    try:
        rownum = 0
        totalnum = 0
        data_list = []
        cur.execute("truncate table commuter_score_tmp")
        sql_insert = "insert into commuter_score_tmp(SCORE,CARD_ID) values(%s,%s)"
        for score in score_list:
            data_list.append(score)
            rownum = rownum + 1
            if (rownum == batch):
                cur.executemany(sql_insert, data_list)
                totalnum = totalnum + rownum
                print(totalnum)
                rownum = 0
                data_list = []
        if len(data_list)!=0:
            cur.executemany(sql_insert, data_list)
            totalnum = totalnum + len(data_list)
            print(totalnum)
        sql_update = "UPDATE commuter_law l LEFT JOIN commuter_score_tmp p ON  l.CARD_ID=p.CARD_ID SET  l.score = p.score where 1=1"
        cur.execute(sql_update)
        cur.execute("COMMIT")
    except Exception as e:
        print(e.message)
        raise e
    finally:
        cur.close()
        connection.close()

        

if __name__ == "__main__":
    print(get_time(),"################Starting Caculate Score####################")
    batch = 1000
    connection,cur = connect()
    id_list = get_ID()
    data_list = get_data()
    
    table_array = np.array([id_list]+data_list).transpose()
    prep_data_to_table(table_array.tolist())
    
    sample_array = get_sample_array(data_list)
    maximum = get_goodandbad_sample(data_list)
    distance_list = get_distance(sample_array,maximum)
    score_list = get_score(distance_list)
    temp_data = []
    temp_data.append(score_list)
    temp_data.append(id_list)
    temp_array = np.array(temp_data)
    score_array = temp_array.transpose()
    update_score(score_array.tolist())
    print(get_time(),"################Complete Caculate Score####################")
    
    
