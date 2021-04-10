#coding=UTF-8
from Configure import *
from c_Compute_Character_Likeness import *
from d_Compute_Date_Characters_Likeness import *
from e_Get_Topk_Likeness import *

from g_Evaluate_Weight import *
from h_Compute_Increase_Index import get_index
import pickle as pl

def transform_timestamp(data_series_chara):
    size=60
    lin = ["%02d:%02d" %(h,m) for h in range(5, 24) for m in range(0, 60, size)]+['24:00'] #5分钟粒度 ['02:00', '02:05', 02:10', ...] （从2:00开始到次日2点(26:00))
    periods = [ '%s-%s'%(lin[i],lin[i+1]) for i in range(len(lin)-1) ]
    data_series_chara[periods]=data_series_chara['分时序列'].apply(lambda x:pd.Series([[i] for i in x]))
    data_series_chara=pd.melt(data_series_chara,id_vars=['line_id','station_id','日期','星期','季节'],\
    value_vars=periods,\
    var_name='时段',value_name='分时序列')
    data_series_chara['分时序列']=data_series_chara['分时序列'].apply(lambda x:np.array(x))
    data_series_chara['时间']=data_series_chara[['日期','时段']].apply(lambda x:x['日期']+'_'+x['时段'],axis=1)
    return data_series_chara

def inference(data_series_chara,date,best_pfm_weight_paras, inference_series_len, share_best_weight=False,top_k=3, algori='mean' ,increase_index = 1):
    #date 要预测的日期（必须存在data_series_chara之中）
    #best_pfm_weight_paras 通过evaluate_weight评估的最优权重参数
    
    #return  预测日期估算的序列（用计算出的前三相似度日期的序列均值作为估计）
    best_pfm_weight_paras = np.squeeze(best_pfm_weight_paras)
    
    print(get_time(),"Start inference date %s"%date)
    train_chara_likeness_data = data_series_chara.loc[(data_series_chara[index_column_name]<date)]
    train_series =  train_chara_likeness_data[series_column_name].values
    series_stack = np.stack(train_series,axis=1)
    chara_multi_likeness = compute_chara_multi_likeness(train_chara_likeness_data)
    compare_date = train_chara_likeness_data[index_column_name]

    inference_info = data_series_chara.loc[(data_series_chara[index_column_name]==date)].iloc[0]
    index = 0 
    for cdate in compare_date:
        compare_info = data_series_chara.loc[(data_series_chara[index_column_name]==cdate)].iloc[0]

        charas_likeness_dict = compute_charas_likeness(data_series_chara,chara_multi_likeness,inference_info,compare_info)
        #date_charas_likeness_dict[(date,cdate)] = charas_likeness_dict
        chara_likeness_list = []
        for chara in chara_sort_by_importance:
            chara_likeness_list.append(charas_likeness_dict[chara])
        chara_likeness_array = np.array(chara_likeness_list)      
        
        if share_best_weight:
            ##各时段共用一组w，best_pfm_weight_paras 使得各时段平均误差之和最小(1*3)
            likeness = np.prod(np.power(chara_likeness_array,best_pfm_weight_paras)) 
            if not isinstance( likeness, np.ndarray):
                likeness = np.repeat(np.array([likeness]), inference_series_len, axis=0)  #防止相似性计算结果为一个数（而非17个）
        else:
            ##各时段用各自的w，best_pfm_weight_paras使得各时段平均误差最小(19*3)
            chara_likeness_list = []
            for n,c in enumerate(chara_likeness_array):
                if not isinstance( c, np.ndarray):
                    chara_likeness_list.append(np.repeat(np.array(c), inference_series_len, axis=0))
                else:
                    chara_likeness_list.append(c)
            chara_likeness_array_new = np.array(chara_likeness_list).T
            likeness = np.prod(np.power(chara_likeness_array_new,best_pfm_weight_paras),axis=1)    
            
        likeness = np.expand_dims(np.array([i.tolist() for i in likeness]), axis=1)  #格式化可变的obeject类型，并增加维度
        #date_likeness_dict[(date,cdate)] = likeness
        if index == 0:
            likeness_stack  = likeness
        else:
            likeness_stack = np.append(likeness_stack, likeness, axis=1)
        index = index +1
        
#     increase_index = get_index(data_series_chara,inference_period[0])
    likeness_stack_topk_index,series_stack_topk_mean,likeness_topk_day_list = get_topk_likeness_mean(compare_date,likeness_stack, series_stack, \
                                                                                                     top_k=top_k, algori=algori)
    
    return series_stack_topk_mean,likeness_topk_day_list


if __name__=='__main__':  
    print("start inference")
    import argparse
    parser = argparse.ArgumentParser(description='manual to this script')
    parser.add_argument('--evaluate_period', type=str, default = None)
    parser.add_argument('--inference_period', type=str, required=True)
    args = parser.parse_args()
    evaluate_period = args.evaluate_period
    inference_period = args.inference_period.split(',')
   
    #evaluate_period = None
    #inference_period = ['20191104','20191213']
    
    data_series_chara = pl.load(open(data_series_chara_path,'rb'))
    
    #下面两行转换成时段因素数据
    data_series_chara = filter_and_fix(data_series_chara, end_date=train_date_scope[1], filter_group='星期', serier_col='分时序列',level=0.999)
    data_series_chara = transform_timestamp(data_series_chara)
    
    all_dates_ts = data_series_chara[index_column_name]
    inference_series_len = len(data_series_chara[series_column_name].iloc[0])   
    
    #根据评估期选出最优权重
    if evaluate_period==None:
        if os.path.exists(weight_evaluate_result_path):
            best_pfm_weight_paras,best_weight_resid_mean = pl.load( open(weight_evaluate_result_path,'rb') )
        else:
            print('please set evaluate_period in condition of No weight_evaluate_result file')
    else:
        weight_paras = pl.load(open(os.path.join(weight_pfm_dir,"weight_paras"),'rb'))
        #评估w的文件路径集合
        evaluate_dates_ts = all_dates_ts[(all_dates_ts>=evaluate_period[0]) & (all_dates_ts<=evaluate_period[-1]) ]
        evaluate_files_path = [ os.path.join(weight_pfm_dir,"weight_pfm_%s"%d) for d in evaluate_dates_ts if "weight_pfm_%s"%d in os.listdir(weight_pfm_dir)]
        best_pfm_weight_paras,best_weight_paras_resid  = evaluate_weight(weight_paras,evaluate_files_path,i=3)#如果用偏差百分比，可能有偏差是inf（5/0=inf）
        print('best_pfm_weight_paras',best_pfm_weight_paras)

    #根据预测期预测
    inference_dates=all_dates_ts[(all_dates_ts>=inference_period[0]) & (all_dates_ts<=inference_period[-1]) ]

    for date in inference_dates:    
        inference_series = inference(data_series_chara, date, best_pfm_weight_paras, inference_series_len )
        data_series_chara.loc[(data_series_chara[index_column_name]==date),'inference'] = str(list(inference_series))
    pl.dump(data_series_chara,open(data_series_chara_inf_path,'wb'))

