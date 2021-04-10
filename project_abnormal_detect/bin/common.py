from sklearn.ensemble import IsolationForest 
import numpy as np

def iforest_abnormal_detect(data,contamination=0.005):
    iforest =  IsolationForest(
                    n_estimators=100, #默认100棵子树
                    max_samples='auto', #子树样本数上限 默认min(256, n_samples)
                    contamination= contamination, #预设异常比例
                    max_features=1.0, #训练子树所用特征数上限
                    bootstrap=False, #子树的训练是否采取重采样策略
                    n_jobs=None, #并行任务数
                    behaviour="new"
                    )
    result = iforest.fit_predict(data)
    anormal_index = np.where(result==-1)
    anormal_value = data[anormal_index].reshape(-1)
    return anormal_value

def iforest_train(train_data,contamination,safe_value):
        #train_data: array
        train_amount = len(train_data)
        #print(train_data)
        if train_amount<=3:
            #print('less then 3 sample')
            return (99999,99999,np.array([]))
            
        iforest_train_data = train_data.reshape(-1,1)
        anormal_value = iforest_abnormal_detect(iforest_train_data,contamination)
        #给出正常值的变化范围
        if len(set(train_data)-set(anormal_value))!=0:
            sup_limit = max(set(train_data)-set(anormal_value))    
        else:
            sup_limit = max(train_data)
        
        sup_limit1=max([safe_value,sup_limit])
               
        return (sup_limit,sup_limit1,anormal_value)
    
def main(concated_string,contamination,safe_value):
    train_data = np.array(eval(concated_string)).reshape(-1)
    sup_limit = iforest_train(train_data,contamination,safe_value)
    return sup_limit
    
    
import datetime as dt
from  datetime import datetime
def get_time():
    nowTime=dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return nowTime
def get_time1():
    nowTime=dt.datetime.now().strftime('%Y%m%d%H%M%S')
    return nowTime


def get_period_by_timesize(size=5):
    dt ={}
    lin = ["%02d:%02d" %(h,m) for h in range(5, 24) for m in range(0, 60, size)]+['24:00'] #5分钟粒度 ['02:00', '02:05', 02:10', ...] （从2:00开始到次日2点(26:00))
    for i in range(len(lin)-1):
        dt[lin[i]] = lin[i+1]
    return dt

import pandas as pd
def get_splitmap_by_timesize(split_period,size=5):
    lin = ["%02d:%02d" %(h,m) for h in range(5, 24) for m in range(0, 60, size)]+['24:00'] #5分钟粒度 ['02:00', '02:05', 02:10', ...] （从2:00开始到次日2点(26:00))
    periods = [ (lin[i],lin[i+1]) for i in range(len(lin)-1) ]
    periods_pd = pd.DataFrame(periods,columns=['start_tm','end_tm'])
    for index ,row in periods_pd.iterrows():
        flag = 0
        for sp in split_period:
            start_time = sp.split('-')[0]
            end_time =  sp.split('-')[1]
            if (row['start_tm']>=start_time) & (row['end_tm']<=end_time):
                periods_pd.loc[index,'nst'] =  start_time
                periods_pd.loc[index,'net'] =  end_time
                flag = 1
        if flag==0:
            periods_pd.loc[index,'nst'] =  row['start_tm']
            periods_pd.loc[index,'net'] =  row['end_tm']
            
    return periods_pd

from pyhive import hive
#kerberos
def get_hive_con():
    data_conn = hive.Connection(host='cdh04.irc.com',
                           port=10000,
                           username='station',
                           database='bmnc_stados', auth='KERBEROS', kerberos_service_name='hive')
    cur = data_conn.cursor()
    return data_conn,cur

#PLAIN
# def get_hive_con():
#     data_conn = hive.Connection(host='cdh04.irc.com',
#                            port=10000,
#                            username='hive',
#                            database='bmnc_stados'
# #                           ,auth='KERBEROS', kerberos_service_name='hive'
#                                )
#     cur = data_conn.cursor()
#     return data_conn,cur

import pyspark
from pyspark import SparkConf, SparkContext
#driver意思为连接spark集群的机子,所以配置host要配置当前编写代码的机子host
#conf = SparkConf().setMaster('spark://10.254.5.101:7077').set('spark.driver.host','10.254.4.106').set('spark.local.ip','10.254.4.106')
def get_spark_sess():
    conf = SparkConf().setMaster('local').setAppName('anormal_detect_compute')
    conf.set("spark.dynamicAllocation.maxExecutors",4) #32
    sc = SparkContext.getOrCreate(conf)
    sess = pyspark.sql.SparkSession(sc)
    return sess



import datetime as dt
def get_tomorrow_daytype():
    now = dt.datetime.now()
    compute_date = (now.date()+dt.timedelta(days=1)).strftime("%Y%m%d")    
    print("select holiday_today,work,holiday_rest from bmnc_stados.bmnc_date_prop t where \
                                                        t.date_id='%s'"%compute_date)
    try:
        data_conn,cur = get_hive_con()
        tomorrow_info = pd.read_sql("select holiday_today,work,holiday_rest from bmnc_stados.bmnc_date_prop t where \
                                                            t.date_id='%s'"%compute_date,data_conn)
        cur.close()
        data_conn.close()
    except Exception as e:
        print(e)
        date_prop = pd.read_csv("../date_prop.csv")
        tomorrow_info = date_prop.loc[date_prop['date_id']==int(compute_date)]
        tomorrow_info = tomorrow_info.reset_index()
    
    if tomorrow_info.loc[0,'holiday_rest'] !=1:
        if tomorrow_info['work'].iloc[0]==1:
            tomorrow='1'
        elif tomorrow_info['work'].iloc[0]==0:
            tomorrow='2'
    elif tomorrow_info.loc[0,'holiday_rest'] ==1:
        tomorrow='3'
        
    return tomorrow
