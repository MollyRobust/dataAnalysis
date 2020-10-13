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

# mysql目标数据库配置信息
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
# 查询hive中的数据
query_sql = """
            select customer_code as CustomerCode,
                   order_number as OrderNumber, 
                   serial_number as SerialNumber, 
                   charge_type as ChargeType, 
                   flow_type as FlowType, 
                   add_time as AddTime
              from fbg_busi.fbg_busi_bsc_warehousbillflow_m
             where add_time >= '2019-01-01 00:00:00' 
               and add_time <= '2020-07-10 00:00:00' 
               and charge_type in (0,2)
             group by customer_code, order_number, serial_number, charge_type, flow_type, add_time
"""
print(query_sql)
res_df = pd.read_sql(query_sql, conn)
print(res_df)

# 查询mysql中orders表，条件为未删除的订单
no_delete_sql = """
                select order_code as ordernumber
                  from orders 
                 where order_status != 0
                 group by order_code
"""
print(no_delete_sql)
no_delete_df = pd.read_sql(no_delete_sql, engine_wms)
print(no_delete_df)

# 过滤未删除的订单
pd.merge(res_df, no_delete_df, how='inner', on='ordernumber')
print(res_df)

# 将结果数据写进mysql数据库
# 1.删除原有数据
del_sql = "delete from %s.%s" % (dstDb, dstTb)
cf.sql_caozuo(del_sql, conn_dest)

# 2.结果更新写入数据库
cf.df_to_sql(engine_dest, res_df, dstTb)


