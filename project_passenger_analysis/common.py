#!/usr/bin/python3
#coding=UTF-8

import os
import glob
import datetime
import pymysql
import pandas as pd

from config import *


def get_time():  
    time_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f : ')
    return time_now 

def get_file_paths(data_dir):
    file_path=[]
    for i in  data_dir:
        file_path=file_path+glob.glob("%s/*"%i) 
    return file_path

def connect():
    connection = pymysql.connect(host=mysql_ip, port=3306, user=mysql_usr, password=mysql_pwd, db=mysql_db, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
    cur = connection.cursor()
    return connection,cur

def connect1():
    connection = pymysql.connect(host=mysql_ip, port=3306, user=mysql_usr, password=mysql_pwd, db=mysql_db, charset='utf8mb4',)
    cur = connection.cursor()
    return connection,cur

def executeScriptsFromFile(filename, cursor):
    fd = open(filename, 'r', encoding='utf-8')
    sqlFile = fd.read()
    fd.close()
    sqlCommands = sqlFile.split(';')
    for command in sqlCommands:
        try:
            print(get_time(),"\n",command)
            cursor.execute(command)
            print(get_time(),"Complete\n")
        except Exception as msg:
            print(msg)
    print('sql执行完成')
 
 
