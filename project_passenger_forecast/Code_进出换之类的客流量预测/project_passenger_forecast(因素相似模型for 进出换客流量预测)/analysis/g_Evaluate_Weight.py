#coding=UTF-8
from Configure import *
import pickle as pl
import numpy as np
def evaluate_weight(weight_paras, evaluate_files_path, weight_evaluate_result_path, detail=False ,i=2, share_best_weight=False):
    #i=2用绝对误差来评价
    #i=3用相对误差来评价
    topk = 1 #权重性能前K组
    weight_pfm_resid_list = []
    for f in evaluate_files_path:
        weight_pfm = pl.load(open(f,'rb'))
        resid = weight_pfm[i]     
        weight_pfm_resid_list.append(resid)
    if i==2:
        weight_pfm_resid_mean = np.round(np.mean(weight_pfm_resid_list,axis=0))
    else:
        weight_pfm_resid_mean = np.mean(weight_pfm_resid_list,axis=0)
    
    if share_best_weight:
        ##各时段共用一组w，best_pfm_weight_paras 使得各时段平均误差之和最小(1*3)
        weight_pfm_resid_topk = np.argpartition(np.sum(weight_pfm_resid_mean,axis=1),topk)[:topk]    
        best_pfm_weight_paras = weight_paras[weight_pfm_resid_topk]  #性能表现第1、2、3组参数组合
        best_weight_resid_mean =  weight_pfm_resid_mean[weight_pfm_resid_topk] #性能表现第1、2、3组参数对应的误差
    else:
        ##各时段用各自的w，best_pfm_weight_paras使得各时段平均误差最小(19*3)
        weight_pfm_resid_topk = np.argpartition(weight_pfm_resid_mean,topk,axis=0)[:topk]
        best_pfm_weight_paras = weight_paras[weight_pfm_resid_topk]  #性能表现第1、2、3组参数组合
        best_weight_resid_mean =  np.partition(weight_pfm_resid_mean,topk,axis=0)[:topk]  #性能表现第1、2、3组参数对应的误差
    
    pl.dump( (best_pfm_weight_paras,best_weight_resid_mean), open(weight_evaluate_result_path,'wb') )
    
    ##打印明细
    if share_best_weight and detail:
        weight_pfm_resid_top1 = np.argmin(np.sum(weight_pfm_resid_mean,axis=1))
        for f in evaluate_files_path:
            predict_date = f.split("_")[-1]
            print("\n#######predict date is %s :"%predict_date)
            weight_pfm = pl.load(open(f,'rb'))
            compare_date = weight_pfm[0]  
            likeness_stack_topk_index = weight_pfm[1]  
            resid_stack = weight_pfm[2]
            resid_percent_stack = weight_pfm[3]

            likeness_topk_index = likeness_stack_topk_index[weight_pfm_resid_top1]
            likeness_topk_day_list = []
            for index in likeness_topk_index :
                likeness_topk_day_list.append(compare_date[index].values.tolist())

            print("Top-k likeness  day     for each time period :\n",likeness_topk_day_list)
            print("\nTop-k likeness  resid   for each time period :\n",resid_stack[weight_pfm_resid_top1])
            print("\nTop-k likeness resid% for each time period :\n",resid_percent_stack[weight_pfm_resid_top1])   
        print('best_weight_resid_mean',best_weight_resid_mean)
      
    return best_pfm_weight_paras,best_weight_resid_mean

if __name__=='__main__':  
    data_series_chara = pl.load(open(data_series_chara_path,'rb'))
    evaluate_period = ['20191008','20191103']
    all_dates_ts = data_series_chara[index_column_name]
    weight_paras = pl.load(open(weight_paras_path,'rb'))
    #评估w的文件路径集合
    evaluate_dates_ts = all_dates_ts[(all_dates_ts>=str(evaluate_period[0])) & (all_dates_ts<=str(evaluate_period[1])) ]
    evaluate_files_path = [ os.path.join(weight_pfm_dir,"weight_pfm_%s"%d) for d in evaluate_dates_ts if "weight_pfm_%s"%d in os.listdir(weight_pfm_dir)]

    evaluate_weight(weight_paras,evaluate_files_path,i=3)

