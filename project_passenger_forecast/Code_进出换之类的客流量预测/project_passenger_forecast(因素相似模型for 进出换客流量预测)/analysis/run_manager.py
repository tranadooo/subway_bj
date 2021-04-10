#coding=utf-8
from f_Train_Character_Weight import *
from b_Pre_Fix import *

import shutil
from joblib import Parallel, delayed

from fromat_combine import *

import os


def train_manager(station,weight_pfm_dir,data_series_chara_path,weight_paras_path,top_k, algori, filter_level=0.999):  
    print(station)
    if station in os.listdir('save_2020'):
        print(get_time(),'%s has already been trained'%station)
        return

    station_path = 'save_2020/%s'%station
    info_path= '%s/%s'%(station_path,'info')
    weight_path= '%s/%s'%(station_path,'weight')
    os.mkdir(station_path)
    os.mkdir(info_path)
    os.mkdir(weight_path)
    
    weight_pfm_dir = station_path+'/'+weight_pfm_dir
    data_series_chara_path = station_path+'/'+data_series_chara_path
    weight_paras_path = station_path+'/'+weight_paras_path

    #########训练###########
    try:
        data_series_chara = all_data.loc[all_data['station_id']==station]
        data_series_chara = data_series_chara.reset_index(drop=True)
        data_series_chara = preprocessing(data_series_chara)

        pl.dump(data_series_chara,open(data_series_chara_path,'wb'))
        weight_paras = np.array(list(itertools.product(weight_para_space, repeat=chara_num)))
        pl.dump(weight_paras,open(weight_paras_path,'wb'))

        inference_series_len = len(data_series_chara[series_column_name].iloc[0])
        train_dates = data_series_chara.loc[data_series_chara["日期"].apply(lambda x:train_date_scope[0]<=x<=train_date_scope[1])]["日期"]
        if len(train_dates)==0:
            shutil.rmtree(station_path)
            return 
        data_series_chara = filter_and_fix(data_series_chara, end_date=train_date_scope[1], filter_group='星期', serier_col='分时序列',level=filter_level)

        train(data_series_chara,train_dates,weight_paras,weight_pfm_dir,top_k=top_k, algori=algori)
        
    except Exception as e:
        shutil.rmtree(station_path)
     
    
    
#coding=utf-8
from b_Pre_Fix import  *
from g_Evaluate_Weight import  *
from inference import  *

import shutil

  
def inference_manager(station, weight_pfm_dir, data_series_chara_path, data_series_chara_inf_path, weight_paras_path, \
               weight_evaluate_result_path,evaluate_period,eval_way,top_k, algori, filter_level=0.999):
    print(station)
    if station not in os.listdir('save_2020'):
        print(get_time(),'%s has not been trained'%station)
        return
        
    station_path = 'save_2020/%s'%station
    weight_pfm_dir = station_path+'/'+weight_pfm_dir
    data_series_chara_path = station_path+'/'+data_series_chara_path
    data_series_chara_inf_path = station_path+'/'+data_series_chara_inf_path
    weight_paras_path = station_path+'/'+weight_paras_path
    weight_evaluate_result_path = station_path+'/'+weight_evaluate_result_path
    
    resid_mean_path = station_path+'/'+'resid_mean'
    #########评估和预测###############
    data_series_chara = pl.load(open(data_series_chara_path,'rb'))
    all_dates_ts = data_series_chara["日期"]
    inference_series_len = len(data_series_chara[series_column_name].iloc[0])

    #评估w的文件路径集合
    weight_paras = pl.load(open(weight_paras_path,'rb'))
    evaluate_dates_ts = all_dates_ts[(all_dates_ts>=evaluate_period[0]) & (all_dates_ts<=evaluate_period[-1]) ]
    evaluate_files_path = [ os.path.join(weight_pfm_dir,"weight_pfm_%s"%d) for d in evaluate_dates_ts if "weight_pfm_%s"%d in os.listdir(weight_pfm_dir)]
    if len(evaluate_files_path)==0:
        return
    best_pfm_weight_paras,best_weight_paras_resid  = evaluate_weight(weight_paras,evaluate_files_path,weight_evaluate_result_path,i=eval_way,share_best_weight=False)

    #根据预测期预测
    inference_dates=all_dates_ts[(all_dates_ts>=inference_period[0]) & (all_dates_ts<=inference_period[-1]) ]

    data_series_chara = filter_and_fix(data_series_chara, end_date=inference_period[0], filter_group='星期', serier_col='分时序列',level=filter_level)
    resid_list = []
    for date in inference_dates:
        series = data_series_chara.loc[(data_series_chara['日期']==date),'分时序列'].iloc[0]
        inference_series,likeness_topk_day_list = inference(data_series_chara, date, best_pfm_weight_paras, inference_series_len,
                                                                                           share_best_weight=False, top_k=top_k, algori=algori )
        data_series_chara.loc[(data_series_chara['日期']==date),'inference'] = str(list(inference_series))
        data_series_chara.loc[(data_series_chara['日期']==date),'best_weight'] = str(best_pfm_weight_paras)
        data_series_chara.loc[(data_series_chara['日期']==date),'likeness_topk_day_list'] = str(likeness_topk_day_list)
        resid_list.append(np.abs(series-inference_series))
        pl.dump(data_series_chara,open(data_series_chara_inf_path,'wb'))
        
    resid_mean = np.array(resid_list).mean(axis=0).astype(int)
    print(resid_mean)
    pl.dump([station,resid_mean],open(resid_mean_path,'wb'))
    print(get_time(),"Complete one")


if __name__=='__main__':
    
    all_data=pl.load(open('train_data_2020','rb'))
    line_station_arr  = all_data[['line_id','station_id']].drop_duplicates().values
    

    shutil.rmtree('save_2020')
    os.mkdir('save_2020')
    
    Parallel(n_jobs=-1)(delayed(
        train_manager
        )(station,weight_pfm_dir,data_series_chara_path,weight_paras_path,top_k, algori) \
                for line,station in line_station_arr)

       
    Parallel(n_jobs=-1)(delayed(
        inference_manager
        )(station, weight_pfm_dir, data_series_chara_path, data_series_chara_inf_path,\
            weight_paras_path,weight_evaluate_result_path,evaluate_period,eval_way,top_k, algori) \
            for line,station in line_station_arr)
        
    file_mark='%s,%s'%(top_k, algori)
    output_detail(file_mark) #格式化并合并所有站内容写入../test/file_mark下