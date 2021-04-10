#!/usr/bin/python3
#coding=UTF-8

import pymysql
import numpy as np
import random
import pandas as pd

import sys
sys.path.append(".")
from common import *


def get_sample_list(table_name, test=False):
    #取出带card_id的通勤特征数据【卡号，常起始站数量，常出行线路数量，常出行时段数量，出行天数】
    try:
        if test:
            sql_show = "select CARD_ID,LAW_STATION_NUM,LAW_LINE_NUM,LAW_TIME_NUM,DAY_NUM from %s limit 1000"%table_name
        else:
            sql_show = "select CARD_ID,LAW_STATION_NUM_PREPED,LAW_LINE_NUM_PREPED,LAW_TIME_NUM_PREPED,DAY_NUM_PREPED from %s"%table_name
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
        return data_list
    except Exception as e:
        print(e.message)
        raise e
    finally:
        cur.close()
        connection.close()

        
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


def get_next_center_index(center_data_array,sample_data_array):
    #新质心生成法
    #对所有样本取离类质心最近的距离（应该是衡量此样本和所有类质心的最大相似度），
   # 然后取其中最大距离对应的样本索引（相当于选相似度最小的作为新质心）
    try:
        max_index = 0
        max_distance = 0

        index = 0
        if center_data_array[0][0] == str:
            center_data_array =  np.array([i[1:] for i in center_data_array])      
        
        for sample_data in sample_data_array:
            min_distance = -1
            for center_data in center_data_array:
                distance = np.sqrt(np.sum(np.square(center_data - sample_data)))
                if (min_distance == -1 or distance < min_distance):
                    min_distance = distance
            if min_distance > max_distance:
                max_distance = min_distance
                max_index = index
            index = index + 1

        return max_index
    except Exception as e:
        print(e.message)
        raise e

        
def get_center_array(sample_data_array,sample_label_array,k):
    #质心生成法
    #对任意样本集，先选取离远点最近的一个点作为质心，然后按新质心生成法生成指定数目的新质心
    #经过优化改为：
    #选取按指标排序后数量的k分点为k个质心，如果质心有重复删除重复质心然后按新质心生成法补全删掉的
    try:
        center_data_list = []
        center_label_list=[]
        index_list=[]
        sample_data_df=pd.DataFrame(sample_data_array)
        n=len(sample_data_array)
        
        for i in range(k):
            if i==0:
                intercept=0
            else:
                intercept =int(i*n/(k-1)-1)
            index = sample_data_df.sort_values(by=[0,1,2,3]).iloc[intercept,:].name
            index_list.append(index) 
            center_data_list.append(list(sample_data_array[index]))
            center_label_list.append(sample_label_array[index])
            
        center_data_array,center_label_array = np.array(center_data_list),np.array(center_label_list)
        
        #检查是否有重复的质心，如果有则删掉重复的，然后新质心生成法补全删掉的质心
        repeat_center_index_list=[]
        for i in range(len(center_data_array)-1):
            for j in range(i+1,len(center_data_array)):
                if (center_data_array[j] == center_data_array[i]).all():
                    repeat_center_index_list.append(j)
        
        if repeat_center_index_list != []:
            repeat_center_index_list=list(set(repeat_center_index_list))
            center_data_array=np.delete(center_data_array,repeat_center_index_list,axis=0)
            center_label_array=np.delete(center_label_array,repeat_center_index_list,axis=0)
            repeat_times=len(repeat_center_index_list)
            for k in range(repeat_times):
                next_center_index=get_next_center_index(center_data_array,sample_data_array)
                center_data_array = np.append(center_data_array, np.expand_dims(sample_data_array[next_center_index], axis=0), axis=0)
                center_label_array = np.append(center_label_array, np.expand_dims(sample_label_array[next_center_index], axis=0), axis=0)
            
        return center_data_array,center_label_array
    except Exception as e:
        print(e.message)
        raise e


def get_owner_center_index(center_data_array,sample_data):
    #对一个点，从A集合选取最近的点索引
    try:
        min_index = 0
        min_distance = -1

        index = 0
        for center_data in center_data_array:
            distance = np.sqrt(np.sum(np.square(center_data - sample_data)))
            if (min_distance == -1 or distance < min_distance):
                min_distance = distance
                min_index = index
            index = index + 1

        return min_index
    except Exception as e:
        print(e.message)
        raise e

        
def first_cluster(sample_data_array,sample_label_array,center_data_array):
    #对一个样本集和质心集，按距离归类
    try:
        
        group_data_dict = {}
        group_label_dict = {}
        for index,center_data in enumerate(center_data_array):
            group_data_dict[index] = []
            group_label_dict[index] = []

        for index,sample_data in enumerate(sample_data_array):
            
            center_index = get_owner_center_index(center_data_array, sample_data)
            group_data_dict[center_index].append(sample_data.tolist())
            group_label_dict[center_index].append(sample_label_array[index].tolist())
        
        for index in group_data_dict:
            group_data_dict[index] = np.array(group_data_dict[index])
            group_label_dict[index] = np.array(group_label_dict[index])

        return group_data_dict, group_label_dict
    except Exception as e:
        print(e.message)
        raise e

        
def split_center(split_data_array,center_array):    
    std = np.std(split_data_array,axis=0)
    max_std = std.max()
    max_std_index = std.argmax()
    center_high = center_array.copy()
    center_high[max_std_index] = center_high[max_std_index] +0.5*max_std
    center_low = center_array.copy()
    center_low[max_std_index] = center_low[max_std_index] -0.5*max_std
    splited_center_array = np.append(np.expand_dims(center_high,axis=0),np.expand_dims(center_low,axis=0),axis=0)
    return  splited_center_array

        
def get_center_data_array(group_data_dict):
    #对所有类组计算质心
    try:
        center_data_list = []
        center_data_array = np.array(center_data_list)

        for index in range(len(group_data_dict)):
            center_data = np.mean(group_data_dict[index], axis=0)
            if index == 0:
                center_data_array = np.expand_dims(center_data, axis=0)
            else:
                center_data_array = np.append(center_data_array, np.expand_dims(center_data, axis=0), axis=0)

        return center_data_array
    except Exception as e:
        print(e.message)
        raise e        
        
        
def cancel_cluster(group_data_dict, group_label_dict, center_data_array, min_n, k):
    #样本最少且小于指定值的组解散
    #对所有类组，选取最少样本数的组，如果样本数小于指定类样本数，则将此组解散并将组内样本按离其它各组质心的距离分到其它组
    try:
        new_group_data_dict = {}
        new_group_label_dict = {}
        new_center_data_array = np.array([])

        cluster_num = len(center_data_array)

        min_index = 0
        min_sample_num = 0

        for index in range(cluster_num):
            sample_num = len(group_data_dict[index])
            if (min_sample_num == 0 or min_sample_num > sample_num):
                min_sample_num = sample_num
                min_index = index
    

        if min_sample_num < min_n:
            new_center_data_array = np.delete(center_data_array, min_index, axis=0)
            new_group_data_dict, new_group_label_dict = first_cluster(sample_data_array, sample_label_array, new_center_data_array )
            new_center_data_array1 = get_center_data_array(new_group_data_dict)
            
        else:
            new_group_data_dict = group_data_dict
            new_group_label_dict = group_label_dict
            new_center_data_array = center_data_array
            new_center_data_array1 = get_center_data_array(new_group_data_dict)

        return new_group_data_dict, new_group_label_dict, new_center_data_array, new_center_data_array1
    except Exception as e:
        print(e.message)
        raise e

        
def split_cluster(group_data_dict, group_label_dict, center_data_array,min_n, k):
    #类内方差最大的一组分裂
    #对所有类组，选取样本数量大于2倍类样本下限中类组方差最大的组，按质心生成法生成两个质心，将此组用这两个质心分裂成两类
    #（这里的分裂与传统的ISODATA有所不同）
    #  1.传统的是指定分裂临界方差而不是不加判断直接对最大方差的组分裂）
    #  2.传统的是将要分裂组的质心，按照标准差最大分量的1/2，对质心相应位置进行+和-分离成两个质心
        # 比如质心为[5,8,7] 标准差[2,1,3] 那么新质心就是[5,8,7+1.5]和[5,8,7-1.5]
    #最新修改：质心分裂法改成传统的分裂方式
    try:
        cluster_num = len(center_data_array)

        max_var = 0
        max_index = 0
        
        for index in range(cluster_num):
            data_array = group_data_dict[index]
            data_var = np.var(data_array)
            if (index == 0 or max_var < data_var) and len(data_array)>2*min_n:
                max_var = data_var
                max_index = index
        

        split_data_array = group_data_dict[max_index]
        split_label_array = group_label_dict[max_index]
        
        new_center_data_array = center_data_array[max_index]        
        new_center_data_array = split_center(split_data_array,new_center_data_array)
        
        center_data_array = np.delete(center_data_array,max_index,0)
        new_center_data_array = np.append(center_data_array, new_center_data_array, axis=0)
        
        new_group_data_dict, new_group_label_dict = first_cluster(sample_data_array, sample_label_array, new_center_data_array )
        new_center_data_array1 = get_center_data_array(new_group_data_dict)
        
        return new_group_data_dict, new_group_label_dict, new_center_data_array, new_center_data_array1
    except Exception as e:
        print(e.message)
        raise e



def merge_cluster(group_data_dict, group_label_dict,center_data_array):
    # 类间距最小的两组合并
    #对所有类组，计算两两之间的质心距离，选取最小质心距离的两个组，进行合并，新质心为合并后的类组质心
    #（这里的分裂与传统的ISODATA有所不同）
    #  1.传统的是指定分裂临界合并类间距而不是不加判断直接对最最小距离的组合并）
    try:
        new_group_data_dict = {}
        new_group_label_dict = {}
        new_center_data_array = np.array([])
        
        cluster_num = len(center_data_array)
        a = 0
        b = 0
        min_distance = 0
        cluster_num = len(center_data_array)

        for x in range(cluster_num):
            for y in range(x):
                if y < x:
                    distance = np.sqrt(np.sum(np.square(center_data_array[x] - center_data_array[y])))
                    if (min_distance == 0 or min_distance > distance):
                        min_distance = distance
                        a = x
                        b = y
        
        if cluster_num > 2:
            
            center_data_array = np.delete(center_data_array,[a,b],0)
            merge_data_array = np.append(group_data_dict[a], group_data_dict[b], axis=0)
            merge_label_array = np.append(group_label_dict[a], group_label_dict[b], axis=0)
            merge_center_data = np.expand_dims(np.mean(merge_data_array, axis=0),axis=0)
            new_center_data_array = np.append(center_data_array, merge_center_data, axis=0)
            
            new_group_data_dict, new_group_label_dict = first_cluster(sample_data_array, sample_label_array, new_center_data_array )
            new_center_data_array1 = get_center_data_array(new_group_data_dict)
            
        else:
            new_group_data_dict = group_data_dict
            new_group_label_dict = group_label_dict
            new_center_data_array = center_data_array
            new_center_data_array1 = get_center_data_array(new_group_data_dict)
            
        return new_group_data_dict, new_group_label_dict, new_center_data_array, new_center_data_array1
    except Exception as e:
        print(e.message)
        raise e

        
def iteration_cluster(group_data_dict, group_label_dict,center_data_array,min_n,k):
    #对类组进行解散--分裂--合并的循环迭代
    try:
        
        new_group_data_dict, new_group_label_dict, new_center_data_array, new_center_data_array1= cancel_cluster(group_data_dict,group_label_dict,center_data_array, min_n,k)
        
        print("解散后：")
        for x in range(len(new_group_data_dict)):
            print(new_center_data_array[x],len(new_group_data_dict[x]),"----",new_center_data_array1[x])
        
        
        new_group_data_dict, new_group_label_dict, new_center_data_array, new_center_data_array1= split_cluster(new_group_data_dict,new_group_label_dict,new_center_data_array1,min_n,k)
        
        print("拆分后：")
        for x in range(len(new_group_data_dict)):
            print(new_center_data_array[x],len(new_group_data_dict[x]),"----",new_center_data_array1[x])
        
        new_group_data_dict, new_group_label_dict, new_center_data_array, new_center_data_array1 = merge_cluster(new_group_data_dict,new_group_label_dict,new_center_data_array1)
        
        print("合并后：")
        for x in range(len(new_group_data_dict)):
            print(new_center_data_array[x],len(new_group_data_dict[x]),"----",new_center_data_array1[x])
            
        return new_group_data_dict, new_group_label_dict, new_center_data_array, new_center_data_array1
    except Exception as e:
        print(e.message)
        raise e

        
def main_cluster(c,k,min_rate,iteration_times,table_name,test):
    try:
        sample_list = get_sample_list(table_name,test)
        sample_array = np.array(sample_list)
        print(sample_array[0])

        global sample_data_array
        global sample_label_array
        
        sample_data_array = sample_array[:, 1:5].astype(float)

        sample_label_array = sample_array[:, 0:1]
        
        ###归一加权处理
        if test:
            scale_list = get_scale_list(sample_data_array.transpose().tolist())
            weight_list = get_feature_weight(scale_list)
            data_preprocess_list = get_data_preprocess_list(scale_list, weight_list)
            sample_data_array = np.array(data_preprocess_list).astype(float).transpose()
            print(sample_data_array[0])

        min_n = round(len(sample_list) * min_rate)

        center_data_array, center_label_array = get_center_array(sample_data_array, sample_label_array, c)
        group_data_dict, group_label_dict = first_cluster(sample_data_array, sample_label_array, center_data_array)
        #center_data_array = get_center_data_array(group_data_dict)  
        #这一步有没有更新质心并没有影响测试结果（因为之后的iteration_cluster中的第一个cancel会更新消弭了这种影响）
        center_data_array_list = []

        for index in range(iteration_times):
            print("\n",get_time(),"################第 %s 次迭代####################"%index)
            group_data_dict, group_label_dict, center_data_array,center_data_array1 = iteration_cluster(group_data_dict, group_label_dict, center_data_array, min_n,k)
            center_data_array_list.append(np.sort(center_data_array1,axis=0))

            if index>=1:
                for i in range(index):
                    if len(center_data_array_list[index])==len(center_data_array_list[i]):
                        if (center_data_array_list[index]==center_data_array_list[i]).all() :
                            print("迭代中止：第%s次迭代质心集在第%s迭代结果中出现过！！！"%(index,i))
                            return group_data_dict, group_label_dict, center_data_array1
        return group_data_dict, group_label_dict, center_data_array1
    except Exception as e:
        print(e.message)
        raise e


def update_cluster(group_label_dict,table_name):
    connection,cur = connect()
    try:
        cur.execute("TRUNCATE table commuter_cluster_tmp")
        total = 0
        cluster_sql = "INSERT INTO commuter_cluster_tmp(CARD_ID,CLUSTER) VALUES(%s,%s)" 
        for class_ in group_label_dict:
            group_class_list = np.c_[group_label_dict[class_],[class_]*len(group_label_dict[class_])].tolist()
            batch_label_list = []
            index = 0
            for label_class in group_class_list:
                batch_label_list.append(label_class)
                index = index+1
                total = total +1
                if index%1000 == 0:
                    print(index)
                    cur.executemany(cluster_sql, batch_label_list)  
                    batch_label_list = []                    
            if len(batch_label_list) != 0:
                cur.executemany(cluster_sql, batch_label_list)  
        print("总个数：",total)
        sql_update = "UPDATE %s l LEFT JOIN commuter_cluster_tmp p ON  l.CARD_ID=p.CARD_ID SET  l.cluster = p.cluster where 1=1" %table_name
        print(sql_update)
        cur.execute(sql_update)
        cur.execute("COMMIT")
    except Exception as e:
        print(e.message)
        raise e
    finally:
        cur.close()
        connection.close()
        
def main(table_name, test, C, K, min_rate, iteration_times):   
    #table_name 选择聚类的数据对象
    #test 是选取所有数据对象还是抽取部分做测试
    #C 初始聚类数目
    #K 指定聚类数目
    #min_rate 最小簇样本数占比
    #iteration_times 迭代次数
    global connection,cur 
    connection,cur = connect()
    
    group_data_dict, group_label_dict, center_data_array1=main_cluster(C,K,min_rate,iteration_times,table_name,test) 

    group_data_dict_sorted = {}
    group_label_dict_sorted = {}
    sort_index = np.argsort(center_data_array1[:,0])
    for i,j in enumerate(sort_index):
        group_data_dict_sorted[i] = group_data_dict.pop(j)
        group_label_dict_sorted[i] = group_label_dict.pop(j)

    print(get_time(),"################Cluster Complete/Begin Update####################")
    
    update_cluster(group_label_dict_sorted,table_name)
    
    print(get_time(),"################Complete Update Cluster####################")
    return group_data_dict_sorted, group_label_dict_sorted, center_data_array1    




if __name__ == "__main__":
    
    print(get_time(),"################Starting Cluster####################")
    
    #main(table_name = table_name ,test = test,C = C,K = K,min_rate = min_rate,iteration_times = iteration_times)
    group_data_dict_sorted, group_label_dict_sorted, center_data_array1=main(table_name = "commuter_law1",test = True,C = 6,K = 4,min_rate = 0.01,iteration_times = 50)
    #main(table_name = "commuter_law2",test = True,C = 6,K = 4,min_rate = 0.01,iteration_times = 50)
    #main(table_name = "commuter_law3",test = True,C = 6,K = 4,min_rate = 0.01,iteration_times = 50)	
    print(get_time(),"################Complete Cluster####################")
