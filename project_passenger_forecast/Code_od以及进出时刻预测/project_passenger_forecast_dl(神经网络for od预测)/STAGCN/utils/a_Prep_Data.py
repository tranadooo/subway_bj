#coding=UTF-8
import numpy as np

#预处理 
def preprocessing(data_series_chara):
    ##修正类型
    data_series_chara["分时序列"] = data_series_chara["分时序列"].apply(lambda x:np.array(eval(x)))
    try:
        data_series_chara["日期"] = data_series_chara["日期"].apply(lambda x:str(x))
        data_series_chara["日期"] = data_series_chara["日期"].apply(lambda x:x.replace('-',''))
        data_series_chara['总量'] = data_series_chara['总量'].astype(int)
    except Exception as e:
        print("Something wrong in Prep :",e)

    """
    data_series_chara['节假日'] = data_series_chara['节假日'].astype(str)
    ##提取最高、最低温度
    data_series_chara["最高温度"] = data_series_chara["温度"].apply(lambda x: int(x.split("/")[0].replace("℃","")))
    data_series_chara["最低温度"] = data_series_chara["温度"].apply(lambda x: int(x.split("/")[1].replace("℃","")))
    data_series_chara.drop(columns=["温度"],inplace=True)
    ##提取白天风力、夜间风力
    data_series_chara["白天风力"] = data_series_chara["风力风向"].apply(lambda x: np.mean([int(i) for i in x.split("/")[0] if i.isdigit()]) )
    data_series_chara["夜间风力"] = data_series_chara["风力风向"].apply(lambda x: np.mean([int(i) for i in x.split("/")[1] if i.isdigit()]) )
    data_series_chara.drop(columns=["风力风向"],inplace=True)
    ##
    data_series_chara["异常"] =  data_series_chara["abnormal"]
    data_series_chara.drop(columns=["abnormal"],inplace=True)
    """
    data_series_chara=data_series_chara.sort_values(by=['日期'])
    return data_series_chara