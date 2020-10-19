#!/usr/bin/python3
# -*- coding:utf-8 -*-

import pandas as pd

import sys
import datetime
from task import common_function as cf

filepath = 'parameter.env'

# 如果有传参，则用传入的时间跑脚本，用于重跑脚本。格式2020-05-01
date_argv = sys.argv[1]
print(date_argv)

if date_argv:
    dtdate = date_argv
else:
    # 当天跑前一天的数据
    dtenddate = datetime.date.today()
    endDate = dtenddate + datetime.timedelta(days=-1)
    dtdate = str(endDate)

fbg_mid_hostname = cf.MySearch(filepath, 'mysql_fbg_mid_hostname')
fbg_mid_port = int(cf.MySearch(filepath, 'mysql_fbg_mid_port'))
fbg_mid_username = cf.MySearch(filepath, 'mysql_fbg_mid_username')
fbg_mid_password = cf.MySearch(filepath, 'mysql_fbg_mid_password')

# 目标数据库
dbdest = 'fbg_mid_dw'
dstable = 't_ret_picking_sum'

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

# 1、删除目标表中当前仓库当天的数据
delete_sql = ("delete from %s where dtdate='%s'" % (dstable, dtdate))
# 调用函数来操作数据库，删除数据
cf.sql_caozuo(delete_sql, conn_dest)

# 2、查询源数据库数据
query_sql = """select p.warehouse_id,p.wp_code,p.dtdate,substr(sorting_time,12,2) as dthour , 
                      count( product_barcode) checkout_sku,
                      count(case when sorting_mode=2 then product_barcode else null end) checkout_pda_sku, 
                      sum(pick_quantity) as checkout_cnt,
                      sum(case when sorting_mode=2 then pick_quantity else 0 end) as checkout_pda_cnt, 
                      sum(case when sorting_mode=1 then pick_quantity else 0 end) as paper_quantity, 
                      sum(case when sorting_mode=3 then pick_quantity else 0 end) as pickanddeliver_quantity, 
                      count(distinct p.picking_code) as pick_cnt, 
                      count(distinct case when sorting_mode=1 then p.picking_code else null end ) as  paper_cnt, 
                      count(distinct case when sorting_mode=2 then p.picking_code else null end ) as  pda_cnt, 
                      count(distinct case when sorting_mode=3 then p.picking_code else null end ) as pickanddeliver_cnt 
                 from t_mid_picking p 
                 join t_mid_picking_detail pd 
                   on p.dtdate=pd.dtdate and p.wp_code=pd.wp_code and p.picking_code=pd.picking_code  
                where p.dtdate='%s'  
                group by p.warehouse_id,p.wp_code,p.dtdate,dthour """ % dtdate

# 结果保存在字典query_df中
query_df = cf.sql_to_df(engine_dest, query_sql)

# 3、将查询出来的数据存入目标表
cf.df_to_sql(engine_dest, query_df, dstable)

