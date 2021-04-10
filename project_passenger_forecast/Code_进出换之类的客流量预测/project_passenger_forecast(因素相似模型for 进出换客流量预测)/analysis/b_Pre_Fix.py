from scipy.stats import shapiro
from scipy.stats import norm

import numpy as np

def filter_and_fix(data_series_chara, end_date, filter_group='星期', serier_col='分时序列',level = 0.995):
    data_series_chara_train = data_series_chara.loc[data_series_chara['日期']<end_date]
    for week in data_series_chara_train[filter_group].drop_duplicates():
        #按星期类型分类过滤修正
        data_series_chara_tmp = data_series_chara_train.loc[data_series_chara_train[filter_group]==week]
        arrs = np.array([i.tolist() for i in data_series_chara_tmp[serier_col].values]).T
        if len(arrs[0])<=3:
            continue
        for train_data in arrs:
            #对此星期的每一个分时进行修正
            #统计学算法(经测试不太适合多团数据的异常检测)
            intervals,method,flush_value_index = interval_estimate(train_data, truncate_percent=90, level = level)
            #print(train_data,'\n', intervals,method,train_data[flush_value_index],'\n' )
            #替换异常
            normal_index = np.delete(np.arange(len(train_data)),flush_value_index[0])
            for i in flush_value_index[0]:
                left_indexs = normal_index[normal_index<i]
                right_indexs = normal_index[normal_index>i]
                if len(left_indexs)==0:
                    replace_index = right_indexs[0]
                elif len(right_indexs)==0:
                    replace_index = left_indexs[-1]
                else:
                    replace_index=[left_indexs[-1],right_indexs[0]]
                train_data[i] = int(train_data[replace_index].mean())
    #         break
    #     break
        data_series_chara_train.loc[data_series_chara_train[filter_group]==week,serier_col] = [str(list(i)) for i in arrs.T]
    data_series_chara_train[serier_col] = data_series_chara_train[serier_col].apply(lambda x: np.array(eval(x)) if type(x)==str else x )
    data_series_chara.loc[data_series_chara['日期']<end_date]=data_series_chara_train
    return data_series_chara

def interval_estimate(train_data,truncate_percent=96, level = 0.995):
    """
    train_data:：异常检测的输入array
    truncate_percent：正态检测的容忍度。取输入的truncate_percent%再做一次正态拟合
    confidence_levels：置信水平
    validate：是否给出验证精度
    """

    all_pass_ = normal_test(train_data)
    p1 = np.percentile(train_data, (100-truncate_percent)/2)
    p2 = np.percentile(train_data, (100+truncate_percent)/2)
    part_sample = train_data[(train_data>=p1) & (train_data<=p2)]
    if len(part_sample)>3:
        pass_ = normal_test(part_sample)
    else:
        pass_ = False
    if (all_pass_ == True) or (pass_ == True):
        if all_pass_ == True:
            fit_sample = train_data  
        else:
            fit_sample = part_sample
        mean = fit_sample.mean()
        std = fit_sample.std()
        interval = [int(mean-std*norm.ppf((1+level)/2)),int(mean+std*norm.ppf((1+level)/2))]

    else:
        interval = box_algorithm(train_data, level, box_divide_num=4)
        interval = format_output(interval)
        if interval[0]==0 and interval[1]==0:
            interval = box_algorithm(train_data, level, box_divide_num=10)
            interval = format_output(interval)
            
    
    if all_pass_ == True:
        method = '(正态拟合法，整体)'
    elif pass_ == True:
        method = '(正态拟合法，%s'%(truncate_percent)+'%)'
    else:
        method = '(箱线法)'
    
    flush_value_index = np.where((train_data<interval[0]) | (train_data>interval[1]))
        
    return interval,method,flush_value_index

def normal_test(test_array):
    if np.unique(test_array)[0]==0:
        return False
    test_pvalue = shapiro(test_array)[1]
    #如果报错“Input data for shapiro has range zero”说明test_array全都是0
    if test_pvalue>=0.05 and test_pvalue<1:
        pass_ = True
    elif test_pvalue<0.05 or test_pvalue==1:
        pass_ = False
    return pass_

def compute_box_coff(confidence_level):
    pf = norm.ppf((1+confidence_level)/2)
    q1 = norm.ppf(0.25)
    q3 = norm.ppf(0.75)
    alpha = (-(q3-q1)+2*pf)/(2*(q3-q1))
    return alpha

def box_algorithm(train_data,confidence_level,box_divide_num=4):     
    q1 = np.percentile(train_data,100/box_divide_num)
    q3 = np.percentile(train_data,(100*box_divide_num-100)/box_divide_num)
    iqr = q3-q1
    alpha = compute_box_coff(confidence_level)
    interval = [int(q1-alpha*iqr),int(q3+alpha*iqr)]
    return interval

def format_output(interval):
    interval[0] = (interval[0]>0)*interval[0]
    return interval