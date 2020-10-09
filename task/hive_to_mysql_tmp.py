import datetime
import pandas as pd
from task import common_function as cf
from impala.dbapi import connect

# hive连接信息
hive_host = "192.168.79.111"  # 当前连接的机器名
hive_port = 10000
db = "fbg_busi"
user = 'root'
password = 'root'
table = 'fbg_busi_bsc_warehousbillflow_m'
# 创建hive连接
conn = connect(host=hive_host, port=hive_port, database=db, auth_mechanism='PLAIN',)

# mysql目标数据库配置信息
filepath = 'parameter.env'
fbg_mid_hostname = cf.MySearch(filepath, 'mysql_fbg_mid_hostname')
fbg_mid_port = int(cf.MySearch(filepath, 'mysql_fbg_mid_port'))
fbg_mid_username = cf.MySearch(filepath, 'mysql_fbg_mid_username')
fbg_mid_password = cf.MySearch(filepath, 'mysql_fbg_mid_password')
dstDb = "fbg_mid_dw"
dstTb = "t_bsc_check_tremonth_flow"

# 构建目标数据库连接字典
conn_dest = dict()
conn_dest["host"] = fbg_mid_hostname
conn_dest["port"] = fbg_mid_port
conn_dest["user"] = fbg_mid_username
conn_dest["passwd"] = fbg_mid_password
conn_dest["db"] = dstDb
conn_dest["charset"] = "utf8"

engine_dest = ("mysql+pymysql://%s:%s@%s:%d/%s?charset=utf8" % (fbg_mid_username, fbg_mid_password,
                                                                fbg_mid_hostname, fbg_mid_port, dstDb))

query_sql = """
            select customer_code as CustomerCode,
                   order_number as OrderNumber, 
                   serial_number as SerialNumber, 
                   charge_type as ChargeType, 
                   flow_type as FlowType, 
                   add_time as AddTime
              from fbg_busi.fbg_busi_bsc_warehousbillflow_m
             where add_time >= '2019-01-01 00:00:00' 
               and add_time <= '2020-06-30 00:00:00' 
               and charge_type in (0,2)
"""
print(query_sql)
res_df = pd.read_sql(query_sql, conn)
print(res_df)
# 将结果数据写进mysql数据库
# 1.删除原有数据
del_sql = "delete from %s.%s" % (dstDb, dstTb)
cf.sql_caozuo(del_sql, conn_dest)

# 2.结果更新写入数据库
cf.df_to_sql(engine_dest, res_df, dstTb)


