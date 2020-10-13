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

# 目标表
dstTb = 'fbg_busi_bsc_check_tremonth_flow'


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
