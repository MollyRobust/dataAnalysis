import datetime
import pandas as pd
from task import common_function as cf

filepath = 'parameter.env'

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

# 源数据库 连接
engine_source = ("mysql+pymysql://%s:%s@%s:%d/?charset=utf8" % (fbg_source_username, fbg_source_password,
                                                                fbg_source_hostname, fbg_source_port))

# 目标数据库
dstDb = 'fbg_mid_dw'
dstTb = 't_bsc_check_tremonth_flow'

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

# 90天前年月
thrDay = datetime.date.today() + datetime.timedelta(days=-90)
thrYearMonth = str(thrDay).replace("-", "")[0:6]
startTime = str(thrDay + datetime.timedelta(days=-1)) + ' 00:00:00'
endTime = str(thrDay) + ' 00:00:00'

# 1.更新三个月之前的历史数据
# 取3月前历史数据的addTime
query_history = ("SELECT OrderNumber,AddTime from %s.%s "
                 "group by OrderNumber,AddTime;") % (dstDb, dstTb)
history_df = pd.read_sql(query_history, engine_dest)
print(history_df)

# 取 每个【分库日期】下所有【订单号】
history_df['database'] = "gc_bsc_amc_" + history_df['AddTime'].apply(lambda x: str(x).replace("-", "")[:6])
db_set = set()
for item in history_df['database']:
    db_set.add(item)
db_list = list(db_set)

# 取更新后的历史数据
df_list = []
for db in db_list:
    db_df = history_df[history_df.database == db]
    orderNum_str = "','".join(list(history_df.OrderNumber))
    query_update = ("""select CustomerCode, OrderNumber, SerialNumber, ChargeType, FlowType, AddTime 
                         from %s.WarehousBillFlow 
                        where AddTime < '%s' and ChargeType=0 and OrderNumber in ('%s')
                        group by CustomerCode, OrderNumber, SerialNumber, ChargeType, FlowType, AddTime """
                    ) % (db, startTime, orderNum_str)
    df_list.append(pd.read_sql(query_update, engine_source))

# 合并dataframe
update_df = pd.concat(df_list)
print(update_df)

# 2.取3个月前 前1天的数据
db_new = 'gc_bsc_amc_%s' % thrYearMonth
query_newDay = ("""select CustomerCode, OrderNumber, SerialNumber, ChargeType, FlowType, AddTime 
                     from %s.WarehousBillFlow
                    where AddTime >='%s' and AddTime < '%s' and ChargeType = 0
                    group by CustomerCode, OrderNumber, SerialNumber, ChargeType, FlowType, AddTime """
                ) % (db_new, startTime, endTime)
newDay_df = pd.read_sql(query_newDay, engine_source)
print(newDay_df)

# 更新历史数据+新增一日数据
result_df = update_df.append(newDay_df)
print(result_df)

# 删除原有数据
del_sql = "delete from %s.%s" % (dstDb, dstTb)
cf.sql_caozuo(del_sql, conn_dest)

# 3.结果更新写入数据库
cf.df_to_sql(engine_dest, result_df, dstTb)
