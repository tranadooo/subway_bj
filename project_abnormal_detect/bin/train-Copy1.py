import os
script_path = os.path.split(os.path.realpath(__file__))[0]
os.chdir(script_path)
import sys

from common import *

import datetime as dt
from  datetime import datetime
def start_time_format(startTime):
    start_time = datetime.strptime(compute_date+'%s:00'%startTime,'%Y%m%d%H:%M:%S')
    start_time = start_time.strftime("%Y%m%d%H%M%S")
    return start_time

def end_time_format(endTime):
    if endTime != '24:00':
        end_time = datetime.strptime(compute_date+'%s:00'%endTime,'%Y%m%d%H:%M:%S')
        end_time = (end_time+dt.timedelta(seconds=-1)).strftime("%Y%m%d%H%M%S")
    else:
        end_time = compute_date+'235959'
    return end_time

import numpy as np
import pandas as pd
from joblib import Parallel, delayed

def get_data_by_sql(date_scope,exclude_date_scope,train_date_type,train_target,train_time_size):
    sqlfile = '../sql/train1.sql'
    file = open(sqlfile,'r')
    sql_content = file.read()


    sql_train = sql_content.replace('datescope',date_scope).\
                                          replace('exclude',exclude_date_scope).\
                                          replace('work',train_date_type).\
                                          replace('target',train_target).\
                                          replace('timesize',train_time_size).\
                                          replace('\n','').split(';')
    print('train sql is: ')
    for sql in sql_train:
        print(sql)
        cur.execute(sql)
    data = cur.fetchall()
    cols = [i[0] for i in cur.description]
    pd_data = pd.DataFrame(data,columns=cols)
    print(get_time(),'fetch over')

    return pd_data


def aggregation_data(pd_data, split_period):
    for p in split_period:
        s=p.split('-')[0]
        e=p.split('-')[1]
        pd_data.loc[(pd_data['start_tm']>=s) & (pd_data['end_tm']<=e),'newtime']= str([s,e])
    pd_data['newtime'] = pd_data[['start_tm','end_tm','newtime']].apply(lambda x: str([x['start_tm'],x['end_tm']]) if pd.isna(x['newtime']) else x['newtime'], axis=1)
    pd_data[['start_tm1','end_tm1']] = pd_data['newtime'].apply(lambda x: pd.Series(eval(x)))
    pd_data = pd_data.rename(columns={'start_tm':'start_tm2','end_tm':'end_tm2','start_tm1':'start_tm','end_tm1':'end_tm'})
    pd_data = pd_data.groupby(['line_id','station_id','start_tm','end_tm'])['qtty'].apply(','.join).reset_index()
    pd_data['qtty']=pd_data['qtty'].apply(lambda x : '[%s]'%x)
    return pd_data



def iforest_func(subset,contamination,safe_value):
    subset["detect_tmp"] = subset["qtty"].apply(lambda x:main(x,contamination,safe_value))
    return subset

def max_func(subset,safe_value):
    subset["detect_tmp"] = subset["qtty"].apply(lambda x:(max(eval(x)),max([safe_value,max(eval(x))]),np.array([])))
    return subset

import sys
def train(pd_data,algorithm_mark,alg_para_list,size,split_period):

    #计算阈值
    print(get_time(),'compute')
    pd_data_grouped = pd_data.groupby(pd_data.index)
    if len(alg_para_list)!=0:
        results = Parallel(n_jobs=30)(delayed(eval('%s_func'%algorithm_mark))(group,*alg_para_list) for name, group in pd_data_grouped)
    else:
        results = Parallel(n_jobs=30)(delayed(eval('%s_func'%algorithm_mark))(group) for name, group in pd_data_grouped)
    pd_data = pd.concat(results)  
    print(get_time(),'computed')

    #统计训练集异常明细
    print(get_time(),"statistic outliners detail")
    pd_data[['threshold','modified','outliners']]=pd_data['detect_tmp'].apply(pd.Series)
    pd_data['train_n'] = pd_data['qtty'].apply(lambda x:len(eval(x)))
    pd_data['outliner_n'] = pd_data['outliners'].apply(lambda x:len(x))
    pd_data['outliner_p'] = pd_data[['outliner_n','train_n']].apply(lambda x:'%0.5f'%(x['outliner_n']/x['train_n']),axis=1)    
    
    #检查是否有聚合，如果有聚合就还原聚合部分
    if split_period!=[]:
        print(get_time(),"revivifiction time period")   
        resplit_pd = get_splitmap_by_timesize(split_period,size=int(size))
        resplit_pd.columns=['nst','net','start_tm','end_tm']
        pd_data = pd.merge(pd_data,resplit_pd,on=['start_tm','end_tm'],how='left')
        pd_data = pd_data.rename(columns={'start_tm':'ost','end_tm':'oet','nst':'start_tm','net':'end_tm'})
        pd_data['start_tm'] = pd_data[['ost','start_tm']].apply(lambda x:x['ost'] if pd.isna(x['start_tm']) else x['start_tm'] ,axis=1)
        pd_data['end_tm'] = pd_data[['oet','end_tm']].apply(lambda x:x['oet'] if pd.isna(x['end_tm']) else x['end_tm'] ,axis=1)


    #补全时段
    print(get_time(),'add miss period')
    periods = get_period_by_timesize(int(size))
    starts = list(periods.keys())
    line_station_arr  = pd_data[['line_id','station_id']].drop_duplicates().values
    for line,station in line_station_arr:
        cur_start = pd_data.loc[pd_data['station_id']==station,'start_tm'].values
        miss_starts = list(set(starts)-set(cur_start))
        if len(miss_starts)!=0:
            for miss_start in miss_starts:
                miss_end = periods[miss_start]
                miss_list = [line,station,miss_start,miss_end,'[]',99999,99999,0,0,0.00000,'']
                rows = pd_data.shape[0]
                pd_data.loc[rows,['line_id','station_id','start_tm','end_tm','qtty','threshold','modified','train_n','outliner_n','outliner_p','outliners']]=miss_list

    pd_data['threshold'] = pd_data['threshold'].astype(int)
    pd_data['modified'] = pd_data['modified'].astype(int)
  
    print(get_time(),'complete handle %s'%size)
    
    return pd_data
    
    

        
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--train_date_scopes",type=str) #训练日期范围
parser.add_argument("--exclude_date_scopes",type=str) #过滤日期范围

parser.add_argument("--train_date_types",type=str) #训练日期类型
parser.add_argument("--train_targets",type=str) #训练指标
parser.add_argument("--train_time_sizes",type=str) #训练指标粒度


import hashlib
import traceback
import time
        

if __name__=='__main__':
    
    data_conn,cur = get_hive_con()
    
    print('\n',get_time(),'Start Job \nset defualt parameters with config file')
    f=open('../conf/train_conf.py','r')
    for i in f:
        print(i)
    sys.path.append('../conf')
    from train_conf import *


    args = parser.parse_args()
    #如果有参数是命令行指定的，那么用它覆盖现有参数
    options = vars(args)
    for arg in options:
        value=options[arg]
        if value is not None:
            if arg=="train_date_scopes":
                locals()[arg]=str(value).split(',')
            elif arg=="exclude_date_scopes":
                locals()[arg]=str(value).split('#')
            else:
                locals()[arg]=eval(value)
            print('specially set parameter with cmd',arg,locals()[arg])

    
    import itertools
    combinations = list(itertools.product(train_date_scopes,exclude_date_scopes,train_date_types,
                                              train_targets,train_time_sizes))

    task_n = len(combinations);count=1

    for date_scope,exclude_date_scope,train_date_type,train_target,train_time_size in combinations:
        
        
        train_stratage = {**train_stratage_config['common'],**train_stratage_config[(train_date_type,train_target,train_time_size)]}       
        sess = get_spark_sess()
        
        print('\n',get_time(),'\n#####start training %s/%s#######'%(count,task_n))
        print( 'date_scope--%s\nexclude_date_scope--%s\
                 \ntrain_date_type--%s\ntrain_target--%s\
                 \ntrain_time_size--%s\n'% 
                 (date_scope,exclude_date_scope,train_date_type,train_target,train_time_size))
        
        
        pd_data_ori = get_data_by_sql(date_scope,exclude_date_scope,train_date_type,train_target,train_time_size)
        pd_data=pd.DataFrame([])
        for k,v in train_stratage.items():
            split_period = v[0]
            if k=='main':
                rest_key = [k for k,v in train_stratage.items() if k!='main']
                rest_key = [[i] if type(i)==str else list(i) for i in rest_key]
                if rest_key == []:
                    cur_data = pd_data_ori
                elif len(rest_key)==1:    
                    cur_data = pd_data_ori.loc[~pd_data_ori['station_id'].isin(rest_key[0])].reset_index(drop=True)
                elif len(rest_key)>1:
                    cur_data = pd_data_ori.loc[~pd_data_ori['station_id'].isin(np.sum(rest_key))].reset_index(drop=True)
            else:
                if type(k)==str:
                    k=[k]
                cur_data = pd_data_ori.loc[pd_data_ori['station_id'].isin(k)].reset_index(drop=True)

            if split_period==[]:
                train_plan_id = '1'
                cur_data['qtty']=cur_data['qtty'].apply(lambda x : '[%s]'%x)
            elif split_period!=[]:
                train_plan_id = '2'
                cur_data = aggregation_data(cur_data, split_period)            
            else:
                print("no stratage configed")

            print('\n',get_time(),k,len(cur_data),v[0],v[1],train_time_size)
            cur_data = train(cur_data,v[1][0],v[1][1:],train_time_size,split_period)
            pd_data = pd.concat([pd_data,cur_data])

        try:         
            main_stratage = train_stratage['main']
        except Exception as e:
            main_stratage = v     
        alg_with_para=",".join([str(i) for i in main_stratage[1]])
        
        if main_stratage[0]==[]:
            train_plan_id = '1'
        else:
            train_plan_id = '2'
        
        id_str = str([date_scope,exclude_date_scope,train_plan_id,train_date_type,\
               train_target,train_time_size,alg_with_para])
        md5str = hashlib.md5(id_str.encode(encoding='UTF-8')).hexdigest()
        task_id = md5str
        #插入训练任务信息表
        train_info_sql = "insert overwrite table bmnc_stados.train_his_record partition(train_id='%s') \
                                                values('%s','%s','%s','%s','%s','%s','%s','%s') \
                                    "%(task_id,get_time(),date_scope,exclude_date_scope,train_plan_id,train_date_type,\
                                       train_target,train_time_size,alg_with_para)
        print(train_info_sql)
        cur.execute(train_info_sql)
        print(get_time(),'insert train record finished')
        #pd_data['date_type'] = train_date_type
        #pd_data['train_target'] = train_target
        #pd_data['train_time_size'] = train_time_size

        pd_data = pd_data[['line_id','station_id','start_tm','end_tm','threshold','modified','train_n','outliner_n','outliner_p','outliners']]
        pd_data['outliners']=pd_data['outliners'].apply(lambda x:str(list(x)).replace('[','').replace(']',''))
        pd_data['threshold'] = pd_data['threshold'].astype(str)
        pd_data['modified'] = pd_data['modified'].astype(str)
        pd_data['train_n'] = pd_data['train_n'].astype(int).astype(str)
        pd_data['outliner_n'] = pd_data['outliner_n'].astype(int).astype(str)
        pd_data['outliner_p']=pd_data['outliner_p'].astype(str)
        
        
        spark_df = sess.createDataFrame(pd_data.values.tolist(), list(pd_data.columns))    
        hdfs_file='hdfs:/user/hive/warehouse/bmnc_stados.db/train_result_detail/train_id=%s'%task_id
        print(get_time(),'Insert write to hdfs %s'%hdfs_file)
        
        for j in range(1,4):
            try:    
                spark_df.write.mode("overwrite").option("delimiter", "|").format('csv').save(hdfs_file)
                print("success in %s times"%j)
                break

            except Exception as e:
                print(e)
                print("#######")
                print(traceback.format_exc())
                time.sleep(3)
        
        print(get_time(),'hdfs file writed')
        cur.execute('msck repair table bmnc_stados.train_result_detail')
        
        count=count+1
        
    print("All task complete")
    
    #导出
    os.system( "python3 -u  load_and_save.py \'%s\'   \
           >> ../logs/load_and_save.log 2>&1" % date_scope
         )
    
    #推送
    from ftp import push_file
    push_file()
 
    


