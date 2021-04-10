#!/usr/bin/python3
#coding=UTF-8

import sys
sys.path.append(".")
from common import *

connection,cur = connect()
executeScriptsFromFile("handle/c_TripChain2CommuterChain.sql",cur)
cur.close()
connection.close()
