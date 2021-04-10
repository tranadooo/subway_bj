#coding=UTF-8
###一、合并所有站点预测结果并格式化
import pandas as pd
import numpy as np
import pickle as pl
import os

from a_Prep_Data import get_time

def format_output1(data_series_chara1,periods):
    #data_series_chara1  ['分时序列','inference'] 都是list
#     data_series_chara1['tmp']=data_series_chara1[['分时序列','inference']].\
#                                         apply(lambda x : ['%s'%list(i) for i in list(np.c_[x['分时序列'],x['inference']])],axis=1)
    data_series_chara1['tmp'] = data_series_chara1[['分时序列','inference','likeness_topk_day_list']].\
                                        apply(lambda x : ['%s'%list(i) for i in list(np.c_[x['分时序列'],x['inference'],[str(i) for i in x['likeness_topk_day_list']]])],axis=1)
    data_series_chara1[periods]=data_series_chara1['tmp'].apply(pd.Series)
    data_series_chara2=pd.melt(data_series_chara1,id_vars=['line_id','station_id','日期','星期','best_weight'],\
            value_vars=periods,\
            var_name='time',value_name='ori_predict')
    data_series_chara2[['ori','predict','likedays']] = data_series_chara2['ori_predict'].apply(lambda x: pd.Series(eval(x)))

    data_series_chara2=data_series_chara2[['日期','星期','line_id','station_id','time','ori','predict','best_weight','likedays']]
    data_series_chara2 = data_series_chara2.sort_values(by=['日期','time'])
    
    return data_series_chara2

def output_detail(file_mark):
    size=60
    lin = ["%02d:%02d" %(h,m) for h in range(5, 24) for m in range(0, 60, size)]+['24:00'] #5分钟粒度 ['02:00', '02:05', 02:10', ...] （从2:00开始到次日2点(26:00))
    periods = [ '%s-%s'%(lin[i],lin[i+1]) for i in range(len(lin)-1) ]
    inf_dirs = ['save_2020/%s'%i for i in os.listdir('save_2020') ]
    station_n=len(inf_dirs)
    inf_detail = pd.DataFrame([])
    for n,d in enumerate(inf_dirs):
        print(get_time(),'######%s/%s'%(n+1,station_n))
        if not os.path.exists('%s/info/data_series_chara_inf'%d):
            continue
        data_series_chara_inf = pl.load(open('%s/info/data_series_chara_inf'%d,'rb'))
        data_series_chara_inf = data_series_chara_inf.loc[~data_series_chara_inf['inference'].isna()]
        data_series_chara_inf['分时序列'] = data_series_chara_inf['分时序列'].apply(lambda x: list(x))
        data_series_chara_inf['inference'] = data_series_chara_inf['inference'].apply(lambda x: eval(x))
        data_series_chara_inf['likeness_topk_day_list']=data_series_chara_inf['likeness_topk_day_list'].apply(lambda x: eval(x))
        
        data_series_chara_inf_fm = format_output1(data_series_chara_inf,periods)

        inf_detail = pd.concat([inf_detail,data_series_chara_inf_fm])

    inf_detail['predict'] = inf_detail['predict'].astype(int)
    inf_detail['ori'] = inf_detail['ori'].astype(int)
    inf_detail['deviation']=inf_detail['predict']-inf_detail['ori']
    inf_detail['deviation_p'] = inf_detail[['deviation','ori']].apply(lambda x: x['deviation']/x['ori'] if x['ori']!=0 else 0,axis=1)
    inf_detail['precision'] = inf_detail['deviation_p'].apply(lambda x: (1-abs(x)))

    station_info=pd.read_csv('../info/station_info.csv',dtype=str)
    inf_detail = pd.merge(inf_detail,station_info,how='left',on=['line_id','station_id'])
    inf_detail = inf_detail[['日期','星期','line_name','station_name','time','predict','ori','deviation','precision','deviation_p','likedays']]
    inf_detail.columns = ['日期','星期','线路','车站','时段','预测值','实际值','预测差值','预测精度','预测差异率','相似日']
    inf_detail.to_excel('../test/test_2020/因素模型预测(%s).xlsx'%file_mark,index=None)
    
    return inf_detail

