#coding=UTF-8
import os
script_path = os.path.split(os.path.realpath(__file__))[0]
os.chdir(script_path)

import sys
import pickle as pl
sys.path.append(".")

import pandas as pd
import numpy as np
import itertools

from Configure import *
from c_Compute_Character_Likeness import *
from d_Compute_Date_Characters_Likeness import *
from e_Get_Topk_Likeness import *


#import pysnooper
#@pysnooper.snoop()
def train(data_series_chara,train_dates,weight_paras,weight_pfm_dir, top_k=3, algori='mean'):
    series_len = len(data_series_chara[series_column_name].iloc[0]) 
    for date in train_dates:
        #date='20180103' 
        curr_file = 'weight_pfm_%s'%date
        if curr_file in os.listdir(weight_pfm_dir):
            continue
        print(get_time(),"Start compute date %s"%date)
        train_chara_likeness_data = data_series_chara.loc[(data_series_chara["日期"]<date)]
        train_series =  train_chara_likeness_data[series_column_name].values
        series_stack = np.stack(train_series,axis=1)
        chara_multi_likeness = compute_chara_multi_likeness(train_chara_likeness_data)
        compare_date = train_chara_likeness_data["日期"]
        
        inference_info = data_series_chara.loc[(data_series_chara["日期"]==date)].iloc[0]
        inference_series = inference_info[series_column_name]

        #date_charas_likeness_dict ={}
        date_likeness_dict = {}
        index = 0 
        for cdate in compare_date:
            #cdate='20160601' 
            #if index%20 == 0 :
                #print("    ",get_time(),"current cdate %s"%cdate)
            compare_info = data_series_chara.loc[(data_series_chara["日期"]==cdate)].iloc[0]

            charas_likeness_dict = compute_charas_likeness(data_series_chara,chara_multi_likeness,inference_info,compare_info)
            #date_charas_likeness_dict[(date,cdate)] = charas_likeness_dict
            chara_likeness_list = []
            for chara in chara_sort_by_importance:
                chara_likeness_list.append(charas_likeness_dict[chara])
            chara_likeness_array = np.array(chara_likeness_list)            

            likeness = np.prod(np.power(chara_likeness_array,weight_paras),axis=1)      
            if not isinstance( likeness[0], np.ndarray):
                likeness = np.repeat(np.expand_dims(likeness,axis=1),series_len,axis=1)  #防止相似性计算结果为一个数（而非17个）
            likeness = np.expand_dims(np.array([i.tolist() for i in likeness]), axis=2)  #格式化可变的obeject类型，并增加维度
            #date_likeness_dict[(date,cdate)] = likeness
            if index == 0:
                likeness_stack  = likeness
            else:
                likeness_stack = np.append(likeness_stack, likeness, axis=2)
            index = index +1   
                     
        #每种参数组合的绝对偏差和相对偏差
        likeness_stack_topk_index,resid,resid_percent = get_topk_likeness_date(inference_series, likeness_stack, series_stack, top_k=top_k, algori=algori) 
        
        print("    ",get_time(),"Complete compute %s"%date)

        weight_pfm =(compare_date,    # pd.Series 所有对比日期，结合第二个结果可查看预测日与对比日期的相似度 
                                        likeness_stack_topk_index,   #(78125,17,3)   所有权重组合下17个时段的前3相似日索引下标 
                                        resid,   #(78125, 17)  所有权重组合下的绝对误差
                                        resid_percent  #(78125, 17) 所有权重组合下的相对误差
                                      )
        pl.dump(weight_pfm,open('%s/weight_pfm_%s'%(weight_pfm_dir,date),'wb'))
        print("dump %s complete"%date)
        del likeness_stack, weight_pfm,compare_date,likeness_stack_topk_index,resid,resid_percent #这里如果不del会出现变量size太大替换过程中长时间占用很多交换内存，程序直接被kill

if __name__=='__main__':
    
    data_series_chara = get_data(input_file_path)
    data_series_chara = preprocessing(data_series_chara) 
    pl.dump(data_series_chara,open(data_series_chara_path,'wb'))
    
    weight_paras = np.array(list(itertools.product(weight_para_space, repeat=chara_num)))
    pl.dump(weight_paras,open(weight_paras_path,'wb'))
    
    inference_series_len = len(data_series_chara[series_column_name].iloc[0])   
    train_dates = data_series_chara.loc[data_series_chara["日期"].apply(lambda x:train_date_scope[0]<=x<=train_date_scope[1])]["日期"]
    
    

    train(data_series_chara,train_dates,weight_paras)
