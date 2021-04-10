#!/usr/bin/python3
#coding=UTF-8

import pymysql
import pandas as pd

import sys
sys.path.append(".")
from common import *

batch = 10000

connection,cur = connect()

sql_insert = 'insert into sta_ic_detail (FLOW_NUM,CARD_ID,CARD_TYPE,TRADE_TYPE,ORG_ID,TRADE_TIME,TRADE_STATION_NAME,\
                    MARK_TIME,MARK_STATION_NAME,LINE_ID,BUS_ID,MARK_LINE_ID,MARK_BUS_ID,DOWN_TIME,DOWN_STATION_NAME,\
                    TRADE_DATE,DRIVER_FACE_ID,POS_ID,TRIP_ID,DIRECTION,TRANSFER_FLAG,TRANSFER_ID)\
                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'

filepath =get_file_paths(data_dir)

def insertfirst_txt(filepath):
    try:          
        for file in filepath:            
            f = open(file, 'r', encoding='UTF-8')
            batchdata = []
            index = 0
            totalnum = 0
            line = f.readline() 
            colnums = len(line.strip('\n').split(',')) - 1     #txt多了一列就-1
            while line:
                index = index + 1
                list = line.strip('\n').split(',')[0:colnums]   #txt过滤最后一列重复的日期
                #list = line.strip('\n').split(',')
                batchdata.append(list)
                if (index == batch):
                    totalnum = totalnum + index
                    cur.executemany(sql_insert, batchdata)
                    print("文件%s第%s条" %(file,totalnum))
                    batchdata = []
                    index = 0
                line = f.readline()
            totalnum = totalnum + index
            cur.executemany(sql_insert, batchdata)
            print(totalnum)
            f.close()
    finally:
        cur.close()
        connection.close()
        
        
def insertfirst_csv(filepath):
    try:          
        for file in filepath:            
            f = open(file, 'r', encoding='UTF-8')
            batchdata = []
            index = 0
            totalnum = 0
            line = f.readline() 
            while line:
                index = index + 1
                list = line.strip('\n').split(',')
                batchdata.append(list)
                if (index == batch):
                    totalnum = totalnum + index
                    cur.executemany(sql_insert, batchdata)
                    print("文件%s第%s条" %(file,totalnum))
                    batchdata = []
                    index = 0
                line = f.readline()
            totalnum = totalnum + index
            cur.executemany(sql_insert, batchdata)
            print(totalnum)
            f.close()
    finally:
        cur.close()
        connection.close()
        
        
def noinsertfirst():
    try:
        batchdata = []
        index = 0
        totalnum = 0
        head = f.readline()
        colnums = len(head.strip('\n').split(',')) - 1
        for line in f:
            index = index + 1
            list = line.strip('\n').split(',')[0:colnums]
            batchdata.append(list)
            if (index == batch):
                totalnum = totalnum + index
                cur.executemany(sql_insert, batchdata)
                print(totalnum)
                batchdata = []
                index = 0
        totalnum = totalnum + index
        cur.executemany(sql_insert, batchdata)
        print(totalnum)
    finally:
        f.close()
        cur.close()
        connection.close()

if __name__=="__main__":
    print(get_time(),"################Starting Loading####################")
    # noinsertfirst()
    if  "txt" in filepath[0]:
        insertfirst_txt(filepath)
    elif  "csv" in filepath[0]:
        insertfirst_csv(filepath)

    print(get_time(),"################Complete Loading####################")
