import pandas as pd
from pyhive import hive
from common import *
import sys
import os


#备份之前的接口文件
print(get_time(),'---start backup previous interface data---')
# sys.exit()


date_types = '1,2' if len(sys.argv) <3 else sys.argv[2]
date_type_list = date_types.split(',')
mappings = {'1':'../data/output','2':'../data/output_sx','3':'../data/output_ho'}
mappings_output = {'1':'../data/工作日','2':'../data/双休日','3':'../data/节假日'}
os.chdir(sys.path[0])


#连接hive读取读取新的接口文件所需全量数据
data_conn,cur = get_hive_con()

if len(sys.argv) <2:
    datescope = '20190101-20200714'
else:
    datescope = sys.argv[1]
print('datescope为%s'%datescope)
sql = open('../sql/load_and_display.sql','r').read().replace("datescope",datescope).replace('$count',str(len(date_type_list)*8))
print(sql)
cur.execute(sql)
tmp = cur.fetchall()
cols = [i[0] for i in cur.description]
cols
df = pd.DataFrame(tmp,columns=cols)
df.head(10)

for i in date_type_list:
    os.system('mv %s/* %s_bak/'%(mappings[i],mappings[i]))
    print('mv %s/* %s_bak/'%(mappings[i],mappings[i]))
# os.system('mv ../data/output/* ../data/output_bak')
# os.system('mv ../data/output_sx/* ../data/output_sx_bak')
print('---backup previous interface data ended---')


import datetime as dt
from datetime import datetime
now = dt.datetime.now()
compute_date = (now.date()+dt.timedelta(days=1)).strftime("%Y%m%d")


def start_time_format(startTime):
    start_time = datetime.strptime(compute_date+'%s:00'%startTime,'%Y%m%d%H:%M:%S')
    start_time = start_time.strftime("%Y%m%d%H%M%S")
    return start_time

def end_time_format(endTime):
    if endTime != '24:00':
        end_time = datetime.strptime(compute_date+'%s:00'%endTime,'%Y%m%d%H:%M:%S')
        end_time = (end_time+dt.timedelta(seconds=-1)).strftime("%Y%m%d%H%M%S")
    else:
        end_time = compute_date+'235959'
    return end_time

#時段标准化
df['new_start_tm'] = df['start_tm'].apply(start_time_format)
df['new_end_tm'] = df['end_tm'].apply(end_time_format)


#df.head()


df_in = df.drop(columns=['line_name','station_name','entry_outliers','exit_outliers','start_tm','end_tm'])
#df_in.head()

df_export = df.drop(columns=['date_id','line_id','station_id','new_start_tm','new_end_tm'])
df_export.columns=['train_date_type','train_size','线路名','站点名','进站量阈值','出站量阈值','进站异常点','出站异常点','开始时间','结束时间']
#df_export.head()

#接口文件写入磁盘
# outdirs =['../data/output/','../data/output_sx/']
# date_types = ['1','2']
train_sizes = ['0005','0015','0030','0060']
table_codes=['D053','D054','D055','D056']
table_names=['FICC_PFW_STN_THRESHOULD5','FICC_PFW_STN_THRESHOULD15','FICC_PFW_STN_THRESHOULD30','FICC_PFW_STN_THRESHOULD60']



print('--------开始写入接口文件数据-----------')
count = 1
for date_type in date_type_list:
    for table_code,table_name,train_size in zip(table_codes,table_names,train_sizes):
        out_file = '%s_%s_%s.dat'%(get_time1(),table_code,table_name)
        tmp_df = df_in[(df_in['train_size'] == train_size) & (df_in['train_date_type']== date_type)].drop(columns=['train_date_type','train_size'])
        if tmp_df.shape[0] > 0:
            tmp_df.to_csv(mappings[date_type]+'/'+out_file,header=0,index=None)
            print('写入第%s个文件%s到%s''成功'%(count, out_file,mappings[date_type]))
            count += 1
        else:
            print('没有第%s个文件%s到%s''数据'%(count, out_file,mappings[date_type]))       
print('-----写入接口文件成功-----')

#写入客户查看文件
# outdirs =['../data/工作日/','../data/双休日/']
# date_types = ['1','2']
train_sizes = ['0005','0015','0030','0060']

table_names=['5分钟粒度','15分钟粒度','30分钟粒度','60分钟粒度']

print('--------开始写入验证文件数据-----------')
count = 1
for date_type in date_type_list:
    for table_name,train_size in zip(table_names,train_sizes):
        out_file = '%s.xlsx'%table_name
        tmp_df = df_export[(df_export['train_size'] == train_size) & (df_export['train_date_type']== date_type)].drop(columns=['train_date_type','train_size'])
        if tmp_df.shape[0] > 0:
            tmp_df.to_excel(mappings_output[date_type]+'/'+out_file,index=None)
            print('写入第%s个文件%s到%s''成功'%(count, out_file,mappings_output[date_type]))
        else:
            print('没有第%s个文件%s到%s''数据'%(count, out_file,mappings_output[date_type]))
        count += 1
print('-------写入验证文件成功-------')