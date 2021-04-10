#coding=UTF-8
import os
from pyhive import hive
import pandas as pd
import re

def get_hive_con():
    data_conn = hive.Connection(host='10.254.5.100',
                           port=10000,
                           username='ETL',
                           database='default')
    cur = data_conn.cursor()
    return data_conn,cur

def get_data(input_file_path):
    if 'sql' in input_file_path:
        data_conn,cur=get_hive_con()
        sql=re.sub('--.*\n',' ',open(input_file_path).read()).replace('\n',' ')
        print(sql)
        data_series_chara=pd.read_sql(sql,data_conn)
    else:
        data_series_chara = pd.read_excel(input_file_path)
    return data_series_chara
        

