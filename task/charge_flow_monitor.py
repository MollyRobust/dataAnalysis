import pandas as pd
from task import common_function as cf
import datetime

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

# 获取当前日期--4号 or 18号
td = datetime.date.today()
td_str = str(td)
thisDay = td_str.split('-')[2]
excx_4 = '04 00:00:00'
excx_18 = '18 00:00:00'

# 确定起止时间月份,时间
# 将4号、其他日期泛化--全都按4号处理
thisYearMonth = td.strftime('%Y%m')
lastYearMonth = (td - datetime.timedelta(days=30)).strftime('%Y%m')
thisTime = td_str[0:8] + excx_4
lastTime = str(td - datetime.timedelta(days=30))[0:8] + excx_18

if thisDay == '18':
    thisYearMonth = td.strftime('%Y%m')
    thisTime = td_str[0:8] + excx_18
    lastTime = td_str[0:8] + excx_4

# 源数据库
srcDbBalan = 'gc_bsc_amc'
srcDbFlowthis = 'gc_bsc_amc_%s' % thisYearMonth
srcDbFlowlast = 'gc_bsc_amc_%s' % lastYearMonth
engine_source = ("mysql+pymysql://%s:%s@%s:%d/?charset=utf8" % (fbg_source_username, fbg_source_password,
                                                                fbg_source_hostname, fbg_source_port))

# 目标数据库
dstDb='fbg_mid_dw'
dstTb='t_bsc_check_charge_balance_flow'
engine_dest=("mysql+pymysql://%s:%s@%s:%d/%s?charset=utf8" % (fbg_mid_username, fbg_mid_password,
                                                              fbg_mid_hostname, fbg_mid_port, dstDb))

# 构建目标数据库连接字典
conn_dest = dict()
conn_dest["host"] = fbg_mid_hostname
conn_dest["port"] = fbg_mid_port
conn_dest["user"] = fbg_mid_username
conn_dest["passwd"] = fbg_mid_password
conn_dest["db"] = dstDb
conn_dest["charset"] = "utf8"

# 当前余额
query_balance = "select AccountCode,CurrencyCode,Balance from gc_bsc_amc.SignBodyBalance"

# 流水--18号 只取本月
query_flow_1 = (""" select wbf.AccountCode, wbf.customercode, wbf.CurrencyCode ,
                           sum(case when wbf.FlowType =0 then Amount else 0 end)-
                           sum(case when wbf.FlowType =1 then Amount else 0 end) as FlowAmount
                      from %s.WarehousBillFlow wbf
                      join %s.WarehouseOrder wo
                        on wbf.OrderNumber = wo.OrderNumber
                     where wo.ChargeType=0 and wbf.AddTime>='%s' and wbf.AddTime< '%s'
                     group by AccountCode,CurrencyCode
""") % (srcDbFlowthis, srcDbFlowthis, lastTime, thisTime)

# 流水--4号 取上月部分和本月部分
query_flow_2 = (""" select AccountCode, CustomerCode, CurrencyCode ,
                           sum(in_amount)-sum(out_amount) as FlowAmount
                      from (
                           select wbf.AccountCode, wbf.CustomerCode, wbf.CurrencyCode ,
                                  case when wbf.FlowType =0 then Amount else 0 end as in_amount,
                                  case when wbf.FlowType =1 then Amount else 0 end as out_amount
                             from %s.WarehousBillFlow wbf
                             join %s.WarehouseOrder wo
                               on wbf.OrderNumber = wo.OrderNumber
                            where wo.ChargeType=0 and wbf.AddTime>='%s'
                           union all
                           select wbf.AccountCode, wbf.CustomerCode, wbf.CurrencyCode ,
                                  case when wbf.FlowType =0 then Amount else 0 end as in_amount,
                                  case when wbf.FlowType =1 then Amount else 0 end as out_amount
                             from %s.WarehousBillFlow wbf
                             join %s.WarehouseOrder wo
                               on wbf.OrderNumber = wo.OrderNumber
                            where wo.ChargeType=0 and wbf.AddTime<'%s'
                           ) tmp
                     group by AccountCode,CurrencyCode
""") % (srcDbFlowlast, srcDbFlowlast, lastTime, srcDbFlowthis, srcDbFlowthis, thisTime)

# 末期余额
query_endbalance = ("""  select bt.AccountCode, bf.CurrencyCode, EndBalance
                           from gc_bsc_amc.BillFunds bf
                           join gc_bsc_amc.Bill b
                             on b.BillId = bf.BillId
                           join gc_bsc_amc.BillTask bt
                             on b.BillTaskId = bt.BillTaskId
                          where b.ExamineStatus = 1 and b.BillId IN
                           ( select Max(BillId)
                               from gc_bsc_amc.Bill
                               join gc_bsc_amc.BillTask
                                 on Bill.BillTaskId=BillTask.BillTaskId
                              where Bill.ExamineStatus=1
                              group by BillTask.AccountCode
                           )
                           group by bt.AccountCode, bf.CurrencyCode
                   """)
df_balance = pd.read_sql(query_balance, engine_source)
df_endbalance = pd.read_sql(query_endbalance, engine_source)

# 将4号、其他日期泛化--全都按4号处理
df_flow = pd.read_sql(query_flow_2, engine_source)

if thisDay == '18':
    df_flow = pd.read_sql(query_flow_1, engine_source)

# 去除dataframe中的空值
df_balance.fillna(value=0, inplace=True)
df_flow.fillna(value=0, inplace=True)
df_endbalance.fillna(value=0, inplace=True)

# 合并数据源
df_balance_flow = pd.merge(df_flow, df_balance, on=('AccountCode', 'CurrencyCode'), how='left')
df_balance_flow.fillna(value=0, inplace=True)
df_ba_flow_endba = pd.merge(df_balance_flow, df_endbalance, on=('AccountCode', 'CurrencyCode'), how='left')
df_ba_flow_endba.fillna(value=0, inplace=True)

# 计算差异金额，并选出差异金额不为0的
df_ba_flow_endba['DifAmount'] = df_ba_flow_endba['EndBalance'] + df_ba_flow_endba['FlowAmount'] - df_ba_flow_endba['Balance']
result_df = df_ba_flow_endba[df_ba_flow_endba.DifAmount != 0]

# 删除今日写入的数据
del_sql = "delete from %s.%s where AddTime >= '%s'" % (dstDb, dstTb, td)
cf.sql_caozuo(del_sql, conn_dest)

# 将结果写入表
cf.df_to_sql(engine_dest, result_df, dstTb)
