import pandas as pd
from task import common_function as cf

filepath = './parameter.env'

# 数据来源，数据库配置信息
fbg_source_hostname = cf.MySearch(filepath, 'mysql_fbg_bsc_hostname')
fbg_source_port = int(cf.MySearch(filepath, 'mysql_fbg_bsc_port'))
fbg_source_username = cf.MySearch(filepath, 'mysql_fbg_bsc_username')
fbg_source_password = cf.MySearch(filepath, 'mysql_fbg_bsc_password')

# 数据中间仓库，数据库配置信息
fbg_mid_hostname = cf.MySearch(filepath, 'mysql_fbg_mid_hostname')
fbg_mid_port = int(cf.MySearch(filepath, 'mysql_fbg_mid_port'))
fbg_mid_username = cf.MySearch(filepath, 'mysql_fbg_mid_username')
fbg_mid_password = cf.MySearch(filepath, 'mysql_fbg_mid_password')

# 源数据库
srcdb = 'gc_bsc_amc_202006'
srctb = 'WarehousBillFlow'
engine_source =("mysql+pymysql://%s:%s@%s:%d/?charset=utf8" % (fbg_source_username, fbg_source_password,
                                                               fbg_source_hostname, fbg_source_port))
print(engine_source)
# 目标数据库
dstdb='fbg_mid_dw'
dsttb='t_tmp_bsc_check_tremonth_flow'

# 构建目标数据库连接字典
conn_dest=dict()
conn_dest["host"] = fbg_mid_hostname
conn_dest["port"] = fbg_mid_port
conn_dest["user"] = fbg_mid_username
conn_dest["passwd"] = fbg_mid_password
conn_dest["db"] = dstdb
conn_dest["charset"] = "utf8"
engine_dest = ("mysql+pymysql://%s:%s@%s:%d/%s?charset=utf8" % (fbg_mid_username, fbg_mid_password,
                                                                fbg_mid_hostname, fbg_mid_port, dstdb))

# 查询历史数据
query_sql = ("SELECT customerCode, orderNumber, serialNumber, ChargeType, flowtype, AddTime"
             " from %s.%s "
             "where addTime >= '2020-06-01 00:00:00' and addTime < '2020-06-17 00:00:00' "
             "and chargeType = 0") % (srcdb, srctb)
print(query_sql)
query_df = pd.read_sql(query_sql,engine_source)
print(query_df)

# 先删除历史数据
del_sql = "delete from %s.%s" % (dstdb, dsttb)
cf.sql_caozuo(del_sql,conn_dest)
# 将结果写入结果表
cf.df_to_sql(engine_dest, query_df, dsttb)
