#coding=UTF-8
from Configure import *
def get_interval_from_str(date1,date2):
    return (datetime.strptime(date1,'%Y%m%d').date() - datetime.strptime(date2,'%Y%m%d').date()).days

#计算定性因素相似度
def compute_chara_multi_likeness(data):    #节假日、星期、天气
    #data 训练定性因素相似度的数据（可以选前两年）
    chara_multi_likeness ={}
    for chara_name in chara_multi:
        chara_values = data[chara_name].unique()
        mean_passen_count_dict = {} 
        for chara_value in chara_values:
            chara_series = data.loc[data[chara_name]==chara_value, series_column_name].mean(axis=0)
            mean_passen_count_dict[chara_value]=chara_series

        mean_passen_count_pd = pd.DataFrame(mean_passen_count_dict)
        max_count = mean_passen_count_pd.max(axis=1)
        min_count = mean_passen_count_pd.min(axis=1)
        chara_multi_likeness_dict = {}
        for i in  range(len(chara_values)):
            for j in range(i+1,len(chara_values)):
                dis = (mean_passen_count_pd[chara_values[i]]-mean_passen_count_pd[chara_values[j]]).apply(lambda x : np.abs(x))
                likeness = (1-dis/(max_count-min_count)).apply(lambda x:round(x,3)).values
                chara_multi_likeness_dict[(chara_values[i],chara_values[j])]=likeness
        
        #将最小的相似度（0）调整为倒数第二小的一半 （不然后续计算日期相似度会为0）
        chara_multi_likeness_pd = pd.DataFrame(chara_multi_likeness_dict).T
        for  i in chara_multi_likeness_pd.columns:
            zero_like_pair_keys =(chara_multi_likeness_pd[i][chara_multi_likeness_pd[i]==0]).keys().values
            second_min_likeness = (chara_multi_likeness_pd[i][chara_multi_likeness_pd[i]!=0]).sort_values().iloc[0]
            for k in zero_like_pair_keys:
                chara_multi_likeness_pd.loc[k,i] = second_min_likeness/2
        for k in chara_multi_likeness_dict.keys():
            chara_multi_likeness_dict[k] = chara_multi_likeness_pd.loc[k].values  
        
        chara_multi_likeness[chara_name] = chara_multi_likeness_dict
        
    return  chara_multi_likeness

#计算定量因素相似度
def compute_chara_conti_likeness1(data,chara_name,value1,value2,critical_val, big_para, small_para ):  #通过分段定义 温度、风力风向 
    if value1>critical_val or value2>critical_val:
        dis = np.abs(value1-value2)*big_para
    else:
        dis =np.abs(value1-value2)*small_para
    dis = (dis<1)*dis+(dis>=1)*1
    likeness = 1- dis
    return likeness
    
def compute_chara_conti_likeness2(data,chara_name):   #通过训练计算 温度、风力风向 
    return compute_chara_multi_likeness(data,chara_name)


#计算日期因素相似度
def compute_chara_date_likeness(date1, date2):  #通过衰减计算 日期
    days_between = get_interval_from_str(date1,date2)
    week_dis = int(days_between/7)
    day_dis = days_between%7
    week_decay = 0.98  #隔两年就相似性极小，两年104周，0.98**104 = 0.122差不多小，可以再调
    day_decay = 0.997 #隔6天 0.997**6=0.982 比隔一周0.98相似性要大一些
    likeness = (week_decay**week_dis)*(day_decay**day_dis)
    return likeness

