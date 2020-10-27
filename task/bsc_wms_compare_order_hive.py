import datetime
import pandas as pd
from task import common_function as cf
from impala.dbapi import connect

filepath = 'parameter.env'

# hive连接信息
hive_host = "192.168.79.111"  # 当前连接的机器名
hive_port = 10000
db = "fbg_busi"
user = 'root'
password = 'root'
table = 'fbg_busi_bsc_warehousbillflow_m'
# 创建hive连接
conn = connect(host=hive_host, port=hive_port, database=db, auth_mechanism='PLAIN',)
