import pandas as pd
from task import common_function as cf

filepath = 'parameter.env'

# 数据来源，数据库配置信息
wms_source_hostname = cf.MySearch(filepath, 'mysql_fbg_wms_hostname')
wms_source_port = int(cf.MySearch(filepath, 'mysql_fbg_wms_port'))
wms_source_username = cf.MySearch(filepath, 'mysql_fbg_wms_username')
wms_source_password = cf.MySearch(filepath, 'mysql_fbg_wms_password')
wmsDb = 'wms_goodcang_com'
engine_wms = ("mysql+pymysql://%s:%s@%s:%d/%s?charset=utf8" % (wms_source_username, wms_source_password,
                                                               wms_source_hostname, wms_source_port, wmsDb))

# 数据来源，数据库配置信息
ods_source_hostname = cf.MySearch(filepath, 'mysql_fbg_ods_hostname')
ods_source_port = int(cf.MySearch(filepath, 'mysql_fbg_ods_port'))
ods_source_username = cf.MySearch(filepath, 'mysql_fbg_ods_username')
ods_source_password = cf.MySearch(filepath, 'mysql_fbg_ods_password')
odsDb = 'gc_ods'
engine_ods = ("mysql+pymysql://%s:%s@%s:%d/%s?charset=utf8" % (ods_source_username, ods_source_password,
                                                               ods_source_hostname, ods_source_port, odsDb))

# 目标数据库配置信息
fbg_mid_hostname = cf.MySearch(filepath, 'mysql_fbg_mid_hostname')
fbg_mid_port = int(cf.MySearch(filepath, 'mysql_fbg_mid_port'))
fbg_mid_username = cf.MySearch(filepath, 'mysql_fbg_mid_username')
fbg_mid_password = cf.MySearch(filepath, 'mysql_fbg_mid_password')
dstDb = 'fbg_mid_dw'
dstTb = 't_ods_fba_orders'
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

# 获取ods订单号
query_ods = "select order_code from orders where order_type = 10"
orders_df = pd.read_sql(query_ods, engine_ods)
print(orders_df)
orders_str = "','".join(list(orders_df.order_code))
print(orders_str)

# wms库的费用详情
query_wms = """select cbl_refer_code, currency_code,
                      case when cbl_type = 2 then cbl_value else 0 end as out_amount,
                      case when cbl_type = 3 then cbl_value else 0 end as in_amount 
                 from customer_balance_log cbl 
                where ft_code = 'FBAO' 
                  and cbl_refer_code in ('%s')""" % orders_str
print(query_wms)
bill_df = pd.read_sql(query_wms, engine_wms)
print(bill_df)

# 将结果写进mid库-先删除再写入
del_sql = "delete from %s.%s" % (dstDb, dstTb)
cf.sql_caozuo(del_sql, conn_dest)
cf.df_to_sql(engine_dest, bill_df, dstTb)
