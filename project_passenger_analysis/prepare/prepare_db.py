#!/usr/bin/python3
#coding=UTF-8

import sys
sys.path.append(".")
from common import *

connection,cur = connect()
executeScriptsFromFile("prepare/truncate_tables.sql",cur)
cur.close()
connection.close()
