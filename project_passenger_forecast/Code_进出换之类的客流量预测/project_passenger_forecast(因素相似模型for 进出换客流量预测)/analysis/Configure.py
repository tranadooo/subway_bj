#coding=UTF-8
from get_data import *
from a_Prep_Data import *

########################配置因素字典#########################
#0-1型因素
chara_01=[
    #"工作"  
    #,"重大政治文化活动"  #暂时没有
     ] 

#定性因素
chara_multi = [
    "星期"
#     ,"季节"
    #,"天气"
    #,"节假日"   
     ]

#连续性（定量）因素
chara_conti = [
    #"最高温度"
    #,"最低温度"
    "日期"
    ,"疫情"
    #,"白天风力"
    #,"夜间风力"
     ]
#连续因素的影响系数分段标定
chara_conti_segpara_dict = {
    #"最高温度":[34,0.2,0.1]
    #,"最低温度":[-4,0.1,0.2]
    #,"白天风力":[3,0.5,0.3]
    #,"夜间风力":[3,0.5,0.3]
    }

#因素的重要性人工排序
#chara_sort_by_importance = ["星期","日期","天气","最高温度","最低温度","白天风力","夜间风力"]
#chara_sort_by_importance = ["星期","日期","季节"]
chara_sort_by_importance = ["疫情","星期","日期"]
chara_num = len(chara_sort_by_importance)


########################配置训练#############################
#输入
 #input_file_path = '../data/天通苑2017_2019因素客流数据.xlsx'
# input_file_path = '../sql/train.sql'  
input_file_path = '../sql/train_2018.sql'  
 
########################配置标识################################
#标识索引字段名
index_column_name = "日期"
#序列字段名
series_column_name = "分时序列"


#######################配置权重参数空间###########################
weight_para_space = [i for i in range(0,5)]


#######################配置训练(默认每次都将当前预测日之前所有的数据用来训练定性因素的相似)#####
#起始时段
# train_date_scope = ['20190801','20191103']
train_date_scope = ['20200615','20200814']

#######################配置评估和预测##########################
#选取以下统计量评估权重
eval_way = 2  #3代表用偏差百分比，2代表绝对偏差
#选取以下时段评估权重
# evaluate_period =  ['20190801','20191101']
evaluate_period =  ['20200605','20200814']
#预测时段
# inference_period = ['20191104','20191213']
inference_period = ['20200815','20200915']


######################训练和预测共享配置########################
#相似日选取个数
top_k=3
#算法
algori='weighted_mean'

#######################输出存储目录配置###########################
#原数据文件
data_series_chara_path = 'info/data_series_chara'
#权重文件
weight_paras_path = 'info/weight_paras'
#权重评估文件目录
weight_pfm_dir = "weight"
#权重评估结果文件
weight_evaluate_result_path = 'info/weight_evaluate_result'
#预测结果文件
data_series_chara_inf_path = 'info/data_series_chara_inf'




