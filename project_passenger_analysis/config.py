#!/usr/bin/python3
#coding=UTF-8

#[paths]
base_dir = r'/root/project_passenger_analysis'
data_dir=["data/sta_ic_detail","data/sta_ic_detail_1"]

#test_section
#data_dir=["data/sta_ic_detail_test"]

#[db_connect]
mysql_ip='10.10.100.197'
mysql_usr='root'
mysql_pwd='123456'
mysql_db='traffic_analyze'

#[cluster]
table_name = "commuter_law"    
#是选取所有数据对象还是抽取部分做测试
test = False
#初始聚类数目
C = 6
#指定聚类数目
K = 4
#最小簇样本数占比
min_rate = 0.01
#迭代次数
iteration_times = 50
