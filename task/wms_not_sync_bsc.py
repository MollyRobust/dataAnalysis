#!/usr/bin/python3
# -*- coding:utf-8 -*-

import pandas as pd
import sys
import datetime

# 添加公用函数文件common_function.py的目录，将公用函数导入
sys.path.append(r'/usr/local/dc-env')
from checkout import common_function as cf

# path=sys.path[0]
filepath = 'parameter.env'
print(filepath)

# 数据来源，数据库配置信息
fbg_wms_hostname = cf.MySearch(filepath, 'mysql_fbg_wms_hostname')
fbg_wms_port = int(cf.MySearch(filepath, 'mysql_fbg_wms_port'))
fbg_wms_username = cf.MySearch(filepath, 'mysql_fbg_wms_username')
fbg_wms_password = cf.MySearch(filepath, 'mysql_fbg_wms_password')
wms_sourceDB = "wms_goodcang_com"

fbg_bsc_hostname = cf.MySearch(filepath, 'mysql_fbg_bsc_hostname')
fbg_bsc_port = int(cf.MySearch(filepath, 'mysql_fbg_bsc_port'))
fbg_bsc_username = cf.MySearch(filepath, 'mysql_fbg_bsc_username')
fbg_bsc_password = cf.MySearch(filepath, 'mysql_fbg_bsc_password')

# 数据中间仓库，数据库配置信息
fbg_mid_hostname = cf.MySearch(filepath, 'mysql_fbg_mid_hostname')
fbg_mid_port = int(cf.MySearch(filepath, 'mysql_fbg_mid_port'))
fbg_mid_username = cf.MySearch(filepath, 'mysql_fbg_mid_username')
fbg_mid_password = cf.MySearch(filepath, 'mysql_fbg_mid_password')

# 目标数据库
dst_db = 'fbg_mid_dw'
dst_tb = 't_bsc_fee_detail_missing_2'

# 3天前
start_date = datetime.date.today() + datetime.timedelta(days=-3)
end_date = start_date + datetime.timedelta(days=+1)
tmp_date = start_date + datetime.timedelta(days=-10)

bsc_sourceDB_set = set()
bsc_sourceDB_set.add('gc_bsc_amc_' + start_date.strftime('%Y%m'))
bsc_sourceDB_set.add('gc_bsc_amc_' + tmp_date.strftime('%Y%m'))
bsc_sourceDB_list = list(bsc_sourceDB_set)

startdate = str(start_date)
enddate = str(end_date)
tmpdate = str(tmp_date)
print(startdate)
print(enddate)
print(tmpdate)

# 构建目标数据库连接字典
conn_dest = dict()
conn_dest["host"] = fbg_mid_hostname
conn_dest["port"] = fbg_mid_port
conn_dest["user"] = fbg_mid_username
conn_dest["passwd"] = fbg_mid_password
conn_dest["db"] = dst_db
conn_dest["charset"] = "utf8"
engine_dest = ("mysql+pymysql://%s:%s@%s:%d/%s?charset=utf8" % (fbg_mid_username, fbg_mid_password,
                                                                fbg_mid_hostname, fbg_mid_port, dst_db))

engine_wms_source = ("mysql+pymysql://%s:%s@%s:%d/%s?charset=utf8" % (fbg_wms_username, fbg_wms_password,
                                                                      fbg_wms_hostname, fbg_wms_port, wms_sourceDB))
engine_bsc_source = ("mysql+pymysql://%s:%s@%s:%d/?charset=utf8" % (fbg_bsc_username, fbg_bsc_password,
                                                                    fbg_bsc_hostname, fbg_bsc_port))

# 1、查询wms签出订单
query_wms_sql = (
            """ select '%s' dtdate, o.customer_code, '订单' as ds_code,
                       o.order_code, bb_charge_time
                  from orders o 
                  join order_operation_time ot 
                    on o.order_id =ot.order_id 
                  left join bil_business_attach bba 
                    on bba.bb_refer_code=o.order_code 
                  left join bil_income bi 
                    on bba.bb_id =bi.bb_id 
                  join bil_business bb 
                    on bba.bb_id = bb.bb_id
                 where bb_charge_time>='%s' and bb_charge_time<'%s' 
                 group by o.warehouse_id, o.customer_code, o.order_code, bb_charge_time, create_currency_code"""
            % (startdate, startdate, enddate))
print(query_wms_sql)
wms_df = pd.read_sql(query_wms_sql, engine_wms_source)
# print(wms_df)

# 2、查询bsc费用明细订单的流水金额
df_list = []
for bsc_sourceDB in bsc_sourceDB_list:
    query_bsc_sql = ("""select OrderNumber as order_code, 
                               AddTime
                          from %s.warehouseorder 
                         where AddTime>='%s'
                         group by OrderNumber, AddTime""" % (bsc_sourceDB, tmp_date))
    print(query_bsc_sql)
    df_list.append(pd.read_sql(query_bsc_sql, engine_bsc_source))
bsc_df = pd.concat(df_list)

# 计算wms费用明细未推送到bsc的订单，保存
total_df = pd.merge(wms_df, bsc_df, how='left', on='order_code')
res_df = total_df[total_df.AddTime.isnull()].drop(columns='AddTime')
print(res_df)
# 删除目标表中数据
del_sql = "delete from %s.%s where dtdate='%s'" % (dst_db, dst_tb, startdate)
print(del_sql)
cf.sql_caozuo(del_sql, conn_dest)

# 写入目标表
cf.df_to_sql(engine_dest, res_df, dst_tb)
