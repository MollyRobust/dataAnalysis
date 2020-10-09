#!/usr/bin/python3
# -*- coding:utf-8 -*-

import pandas as pd
import pymysql
import sys
import datetime
from sqlalchemy import create_engine

# 添加公用函数文件common_function.py的目录，将公用函数导入
sys.path.append(r'/usr/local/dc-env')
import common_function as cf

# path=sys.path[0]
filepath = '/usr/local/dc-env/parameter.env'
print(filepath)

# 如果有传参，则用传入的时间跑脚本，用于重跑脚本。格式2020-05-01
date_argv = sys.argv[1]
print(date_argv)

if date_argv:
    warehouse_date = date_argv
else:
    # 当天跑前一天的数据
    dtenddate = datetime.date.today()
    endDate = dtenddate + datetime.timedelta(days=-1)
    warehouse_date = str(endDate)

# 数据来源，数据库配置信息
fbg_wms_hostname = cf.MySearch(filepath, 'mysql_fbg_wms_hostname')
fbg_wms_port = int(cf.MySearch(filepath, 'mysql_fbg_wms_port'))
fbg_wms_username = cf.MySearch(filepath, 'mysql_fbg_wms_username')
fbg_wms_password = cf.MySearch(filepath, 'mysql_fbg_wms_password')

fbg_bsc_hostname = cf.MySearch(filepath, 'mysql_fbg_bsc_hostname')
fbg_bsc_port = int(cf.MySearch(filepath, 'mysql_fbg_bsc_port'))
fbg_bsc_username = cf.MySearch(filepath, 'mysql_fbg_bsc_username')
fbg_bsc_password = cf.MySearch(filepath, 'mysql_fbg_bsc_password')

# 数据中间仓库，数据库配置信息

fbg_mid_hostname = cf.MySearch(filepath, 'mysql_fbg_mid_hostname')
fbg_mid_port = int(cf.MySearch(filepath, 'mysql_fbg_mid_port'))
fbg_mid_username = cf.MySearch(filepath, 'mysql_fbg_mid_username')
fbg_mid_password = cf.MySearch(filepath, 'mysql_fbg_mid_password')

# print(type(fbg_wms_hostname))

# 目标数据库
dbdest = 'fbg_mid_dw'
wms_sourceDB = "wms_goodcang_com"

# bsc按月份分库 gc_bsc_amc_202004

today_date = datetime.date.today()
dtdate_date = today_date + datetime.timedelta(days=-1)
year = dtdate_date.strftime('%Y')
month = dtdate_date.strftime('%m')
bsc_sourceDB = 'gc_bsc_amc_' + year + month

dtdate = str(dtdate_date)
today = str(today_date)

# 构建目标数据库连接字典
conn_dest = dict()
conn_dest["host"] = fbg_mid_hostname
conn_dest["port"] = fbg_mid_port
conn_dest["user"] = fbg_mid_username
conn_dest["passwd"] = fbg_mid_password
conn_dest["db"] = dbdest
conn_dest["charset"] = "utf8"
engine_dest = ("mysql+pymysql://%s:%s@%s:%d/%s?charset=utf8" % (
fbg_mid_username, fbg_mid_password, fbg_mid_hostname, fbg_mid_port, dbdest))

engine_source = ("mysql+pymysql://%s:%s@%s:%d/%s?charset=utf8" % (
fbg_wms_username, fbg_wms_password, fbg_wms_hostname, fbg_wms_port, wms_sourceDB))
engine_bsc_source = ("mysql+pymysql://%s:%s@%s:%d/%s?charset=utf8" % (
fbg_bsc_username, fbg_bsc_password, fbg_bsc_hostname, fbg_bsc_port, bsc_sourceDB))

# 表一：同步wms签出订单
dstable1 = 'temp_wms_sign_orders'
# 1、删除目标表中当前仓库的数据
delete_sql = ("delete from %s.%s " % (dbdest, dstable1))
# 调用函数来操作数据库，删除数据
cf.sql_caozuo(delete_sql, conn_dest)

# 2、查询源数据库数据
query_sql = (
            " select warehouse_id,o.customer_code,'订单' as ds_code,o.order_code,ship_time ,create_currency_code,sum(bi_amount) order_amount "
            " from orders o "
            " join order_operation_time ot on o.order_id =ot.order_id "
            " left join bil_business_attach bba on  bba.bb_refer_code=o.order_code "
            " left join bil_income bi on bba.bb_id =bi.bb_id "
            " where ship_time>='%s' and ship_time<'%s' "
            " group by warehouse_id,o.customer_code,o.order_code,ship_time ,create_currency_code"
            % (dtdate, today))

# 结果保存在字典query_df中
query_df = cf.sql_to_df(engine_source, query_sql)

# 3、将查询出来的数据存入目标表
cf.df_to_sql(engine_dest, query_df, dstable1)

# 表二：同步表bsc费用明细订单的流水金额
dstable2 = 'temp_bsc_warehouseorder'
# 1、删除目标表中当前仓库的数据
delete_sql2 = ("delete from %s.%s " % (dbdest, dstable2))
# 调用函数来操作数据库，删除数据
cf.sql_caozuo(delete_sql2, conn_dest)

# 2、查询源数据库数据
query_sql2 = ("select wo.OrderNumber , "
              " sum(case when FlowType=0 then Amount else 0 end ) as in_money,"
              " sum(case when FlowType=1 then Amount else 0 end ) as out_money"
              " from %s.warehouseorder wo "
              " left join %s.warehousbillflow  wb on wo.OrderNumber=wb.OrderNumber "
              " where BillingTime>='%s' and BillingTime<'%s'  "
              " group by wo.OrderNumber"
              % (bsc_sourceDB, bsc_sourceDB, dtdate, today))

# 结果保存在字典query_df中
query_df2 = cf.sql_to_df(engine_bsc_source, query_sql2)

# 3、将查询出来的数据存入目标表
cf.df_to_sql(engine_dest, query_df2, dstable2)

# 表三：计算wms费用明细未推送到bsc的订单，保存
dstable3 = 't_bsc_fee_detail_missing'
# 1、删除目标表中当前仓库的数据
delete_sql3 = ("delete from %s.%s where dtdate=%s" % (dbdest, dstable3, dtdate))
# 调用函数来操作数据库，删除数据
cf.sql_caozuo(delete_sql3, conn_dest)

# 2、查询源数据库数据
query_sql3 = ("insert into t_bsc_fee_detail_missing(dtdate,customer_code,ds_code,order_code,ship_time) "
              " select '%s' dtdate,customer_code,ds_code,order_code,ship_time "
              " from temp_wms_sign_orders wo "
              " left join temp_bsc_warehouseorder bo on wo.order_code =bo.OrderNumber "
              " where bo.OrderNumber is null "
              % (dtdate))

# 调用函数来操作数据库，删除数据
cf.sql_caozuo(query_sql3, conn_dest)

# 表四：WMS费用明细总金额与此单据在BSC中的流水的汇总金额（扣款与入款相加）数据不一致
dstable4 = 't_wms_orderfee_bsc_warehousbillflow'
# 1、删除目标表中当前仓库的数据
delete_sql3 = ("delete from %s.%s where dtdate=%s" % (dbdest, dstable4, dtdate))
# 调用函数来操作数据库，删除数据
cf.sql_caozuo(delete_sql3, conn_dest)

# 2、查询源数据库数据
query_sql4 = (
            "insert into t_wms_orderfee_bsc_warehousbillflow(dtdate,customer_code,ds_code,order_code,create_currency_code,ship_time,wms_money,in_money,out_money) "
            " select '%s' dtdate,customer_code,ds_code,order_code,create_currency_code,ship_time, "
            " wo.order_amount as wms_money,in_money,out_money"
            " from temp_wms_sign_orders wo "
            " left join temp_bsc_warehouseorder bo on wo.order_code =bo.OrderNumber "
            " where wo.order_amount != bo.out_money-bo.in_money"
            % (dtdate))

# 调用函数来操作数据库，删除数据
cf.sql_caozuo(query_sql4, conn_dest)
