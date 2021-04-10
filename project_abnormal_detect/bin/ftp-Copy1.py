from ftplib import FTP 
import os 
import sys
sys.path.append('../conf')
from ftp_conf import *

def ftp_up(filename_list): 
    ftp=FTP() 
#     ftp.set_debuglevel(2)#打开调试级别2，显示详细信息;0为关闭调试信息 
#     ftp.connect('10.254.4.106',21)#连接 
#     ftp.login('ftptest','ftptest')#登录，如果匿名登录则用空串代替即可 
#     print(ftp.getwelcome())#显示ftp服务器欢迎信息 
#     ftp.cwd('/home/project_passenger/ftptest/') #选择操作目录 

#读取配置文件的ftp参数
#     ftp.set_debuglevel(2)#打开调试级别2，显示详细信息;0为关闭调试信息 
    ftp.connect(ftp_address,21)#连接 
    ftp.login(ftp_username,ftp_password)#登录，如果匿名登录则用空串代替即可 
    print(ftp.getwelcome())#显示ftp服务器欢迎信息 
    ftp.cwd(ftp_dir) #选择操作目录 
    


    """查看文件
    ftp.dir()
    """
    """删除文件
    ftp.delete('abnormal_detect.ipynb') 
    
    #删除所有文件
    dir_res=[]
    ftp.dir('.', dir_res.append)
    for i in [i.split(' ')[-1] for i in dir_res]:
        ftp.delete(i) 
    """
    """下载文件
    file_handler = open(filename,'wb').write #以写模式在本地打开文件 
    ftp.retrbinary('RETR %s' % os.path.basename(filename),file_handler,bufsize)#接收服务器上文件并写入本地文件 
    """
    bufsize = 1024#设置缓冲块大小 
    for filename in filename_list:
        file_handler = open(filename,'rb')#以读模式在本地打开文件 
        ftp.storbinary('STOR %s' % os.path.basename(filename),file_handler,bufsize)#上传文件 
        ftp.set_debuglevel(0) 
        file_handler.close() 
    ftp.quit() 
    print("ftp up OK")

      

def create_chk_file(up_file):
    file_size = os.path.getsize(up_file)
    file_lines = len(open(up_file, 'r').readlines())
    file_time = up_file.split('_')[0]
    chk_file = up_file.split('.')[0]+'.chk'
    with open(chk_file,'w')as f:
        f.write('%s,%s,%s,%s\n'%(up_file,file_size,file_lines,file_time))
    return chk_file

def create_chk_and_ftp(fl):     
    try:
        for up_file in fl:
            chk_file = create_chk_file(up_file)
            ftp_up([up_file,chk_file])
            print('%s success uploaded'%up_file)
            #shutil.move(up_file,'../bak/')
            print('%s success uploaded'%chk_file)
            #shutil.move(chk_file,'../bak/')
    except Exception as e:
        print(e)

import datetime as dt
        
import shutil
from pyhive import hive
import pandas as pd

from common import *


def push_file():
    now = dt.datetime.now()
    compute_date = (now.date()+dt.timedelta(days=1)).strftime("%Y%m%d")
    
    data_conn,cur = get_hive_con()
    tomorrow_info = pd.read_sql("select holiday_today,work,holiday_rest from bmnc_stados.bmnc_date_prop t where t.date_id='%s'"%compute_date,data_conn)

#     if tomorrow_info.loc[0,'holiday_rest'] !=1:
    if tomorrow_info['work'].iloc[0]==1:
        print('cd ../data/output')
        print(os.listdir())
        os.chdir('../data/output')
        fl = os.listdir()
        fl = [i for i in fl if i.endswith('dat')]
        print(fl)
        print('tomorrow is workday ---start upload workday threshold---')
        create_chk_and_ftp(fl)
    elif tomorrow_info['work'].iloc[0]==0:
        print('cd ../data/output_sx')
        os.chdir('../data/output_sx')
        fl = os.listdir()
        fl = [i for i in fl if i.endswith('dat')]
        print(fl)
        print('tomorrow is weekend ---start upload weekend threshold---')
        create_chk_and_ftp(fl)
#     else:
#         print('holiday neednt upload')
    
if __name__=='__main__':    
    push_file()

# 临时测试，不做判断推送双休日数据
# print('cd ./data/output')
# os.chdir('./data/output')
# fl = os.listdir()
# fl = [i for i in fl if i.endswith('dat')]
# print(fl)
# print('tomorrow is workday ---start upload workday threshold---')
# create_chk_and_ftp(fl)
