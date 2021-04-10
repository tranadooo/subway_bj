#coding=utf-8
import numpy as np
import pandas as pd



def get_index(df,infer_start_date):
    df_train = df[(df['日期'] < infer_start_date)]
    df_eval = df[df['日期'] >= infer_start_date]

    arr_eval = df_eval['分时序列'].values
    arr_train = df_train['分时序列'].values

    res = np.mean(arr_eval, axis=0) / np.mean(arr_train, axis=0)
    res[0],res[18] = 1,1 #开始时段跟结束时段不做缩放
    return res

