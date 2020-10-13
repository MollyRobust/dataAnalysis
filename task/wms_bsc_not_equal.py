#!/usr/bin/python3
# -*- coding:utf-8 -*-

import pandas as pd
import pymysql
import sys
import datetime
from sqlalchemy import create_engine
from task import common_function as cf

filepath = 'parameter.env'

# 配置时间
end_date = datetime.date.today() + datetime.timedelta(days=-1)
start_date = end_date + datetime.timedelta(days=-1)
tmp_date = start_date + datetime.timedelta(days=-5)

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

# 数据来源，数据库配置信息
fbg_wms_hostname = cf.MySearch(filepath, 'mysql_fbg_wms_hostname')
fbg_wms_port = int(cf.MySearch(filepath, 'mysql_fbg_wms_port'))
fbg_wms_username = cf.MySearch(filepath, 'mysql_fbg_wms_username')
fbg_wms_password = cf.MySearch(filepath, 'mysql_fbg_wms_password')
wms_sourceDB = "wms_goodcang_com"
engine_wms_source = ("mysql+pymysql://%s:%s@%s:%d/%s?charset=utf8" % (fbg_wms_username, fbg_wms_password,
                                                                      fbg_wms_hostname, fbg_wms_port, wms_sourceDB))

fbg_bsc_hostname = cf.MySearch(filepath, 'mysql_fbg_bsc_hostname')
fbg_bsc_port = int(cf.MySearch(filepath, 'mysql_fbg_bsc_port'))
fbg_bsc_username = cf.MySearch(filepath, 'mysql_fbg_bsc_username')
fbg_bsc_password = cf.MySearch(filepath, 'mysql_fbg_bsc_password')

engine_bsc_source = ("mysql+pymysql://%s:%s@%s:%d/?charset=utf8" % (fbg_bsc_username, fbg_bsc_password,
                                                                    fbg_bsc_hostname, fbg_bsc_port))

# 数据中间仓库，数据库配置信息
fbg_mid_hostname = cf.MySearch(filepath, 'mysql_fbg_mid_hostname')
fbg_mid_port = int(cf.MySearch(filepath, 'mysql_fbg_mid_port'))
fbg_mid_username = cf.MySearch(filepath, 'mysql_fbg_mid_username')
fbg_mid_password = cf.MySearch(filepath, 'mysql_fbg_mid_password')
# 目标数据库
dbdest = 'fbg_mid_dw'
# 构建目标数据库连接字典
conn_dest = dict()
conn_dest["host"] = fbg_mid_hostname
conn_dest["port"] = fbg_mid_port
conn_dest["user"] = fbg_mid_username
conn_dest["passwd"] = fbg_mid_password
conn_dest["db"] = dbdest
conn_dest["charset"] = "utf8"
engine_dest = ("mysql+pymysql://%s:%s@%s:%d/%s?charset=utf8" % (fbg_mid_username, fbg_mid_password,
                                                                fbg_mid_hostname, fbg_mid_port, dbdest))

# 表一：同步wms签出订单
dstWmsSign = 'temp_wms_sign_orders_2'
# 1、删除数据
delete_sql = ("delete from %s.%s " % (dbdest, dstWmsSign))
cf.sql_caozuo(delete_sql, conn_dest)

# 2、查询wms源数据库数据
wms_sql = (
            """ select warehouse_id,o.customer_code,'订单' as ds_code,
                       o.order_code,ship_time ,create_currency_code,
                       sum(bi_amount) order_amount 
                  from orders o 
                  join order_operation_time ot 
                    on o.order_id =ot.order_id 
                  left join bil_business_attach bba 
                    on bba.bb_refer_code=o.order_code 
                  left join bil_income bi 
                    on bba.bb_id =bi.bb_id 
                 where ship_time>='%s' and ship_time<'%s' 
                 group by warehouse_id,o.customer_code,o.order_code,ship_time ,create_currency_code"""
            % (startdate, enddate))
wms_df = pd.read_sql(wms_sql, engine_wms_source)

# 3、将查询出来的数据存入目标表
cf.df_to_sql(engine_dest, wms_df, dstWmsSign)

# 表二：同步表bsc费用明细订单的流水金额
dstBscFlow = 'temp_bsc_warehouseorder_2'
# 删除数据
delete_sql2 = ("delete from %s.%s " % (dbdest, dstBscFlow))
cf.sql_caozuo(delete_sql2, conn_dest)

# 2、查询源数据库数据
df_list = []
for bsc_sourceDB in bsc_sourceDB_list:
    query_bsc_sql = ("""select wo.OrderNumber , 
                               sum(case when FlowType=0 then Amount else 0 end ) as in_money,
                               sum(case when FlowType=1 then Amount else 0 end ) as out_money
                          from %s.warehouseorder wo 
                          left join %s.warehousbillflow wb 
                            on wo.OrderNumber=wb.OrderNumber 
                         where wo.AddTime>='%s'
                         group by wo.OrderNumber""" % (bsc_sourceDB, bsc_sourceDB, tmp_date))
    print(query_bsc_sql)
    df_list.append(pd.read_sql(query_bsc_sql, engine_bsc_source))
bsc_df = pd.concat(df_list)

# 3、将查询出来的数据存入目标表
cf.df_to_sql(engine_dest, bsc_df, dstBscFlow)

# 表三：WMS费用明细总金额与此单据在BSC中的流水的汇总金额（扣款与入款相加）数据不一致
dst_tb = 't_wms_orderfee_bsc_warehousbillflow_2'
# 1、删除目标表中当前仓库的数据
delete_sql3 = ("delete from %s.%s where dtdate='%s'" % (dbdest, dst_tb, startdate))
# 调用函数来操作数据库，删除数据
cf.sql_caozuo(delete_sql3, conn_dest)

# 2、查询源数据库数据
query_sql4 = ("""insert into t_wms_orderfee_bsc_warehousbillflow_2 (dtdate, customer_code, ds_code, order_code,
                 create_currency_code, ship_time, wms_money, in_money, out_money)
                 select '%s' dtdate, customer_code, ds_code, order_code, create_currency_code, ship_time,
                        wo.order_amount as wms_money, in_money, out_money
                   from temp_wms_sign_orders_2 wo
              left join temp_bsc_warehouseorder_2 bo
                     on wo.order_code =bo.OrderNumber
                  where wo.order_amount != bo.out_money - bo.in_money""" % startdate)
print(query_sql4)
cf.sql_caozuo(query_sql4, conn_dest)
