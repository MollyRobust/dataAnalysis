#!/usr/bin/python3
# -*- coding:utf-8 -*-

import pandas as pd
import pymysql
import sys
import datetime
from sqlalchemy import create_engine
from task import common_function as cf

filepath = 'parameter.env'


dtenddate = datetime.date.today()
endDate = dtenddate + datetime.timedelta(days=-1)
warehouse_date = str(endDate)

# 数据来源，数据库配置信息
fbg_owms_hostname = cf.MySearch(filepath, 'mysql_fbg_owms_hostname')
fbg_owms_port = int(cf.MySearch(filepath, 'mysql_fbg_owms_port'))
fbg_owms_username = cf.MySearch(filepath, 'mysql_fbg_owms_username')
fbg_owms_password = cf.MySearch(filepath, 'mysql_fbg_owms_password')

fbg_hms_hostname = cf.MySearch(filepath, 'mysql_fbg_hms_hostname')
fbg_hms_port = int(cf.MySearch(filepath, 'mysql_fbg_hms_port'))
fbg_hms_username = cf.MySearch(filepath, 'mysql_fbg_hms_username')
fbg_hms_password = cf.MySearch(filepath, 'mysql_fbg_hms_password')

# 数据中间仓库，数据库配置信息

fbg_mid_hostname = cf.MySearch(filepath, 'mysql_fbg_mid_hostname')
fbg_mid_port = int(cf.MySearch(filepath, 'mysql_fbg_mid_port'))
fbg_mid_username = cf.MySearch(filepath, 'mysql_fbg_mid_username')
fbg_mid_password = cf.MySearch(filepath, 'mysql_fbg_mid_password')

# print(type(fbg_owms_hostname))

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

# 首先判断当前执行时间 确定需要统计的仓库
t = datetime.datetime.now().strftime('%H')+':00:00'
t_date = datetime.date.today()

# 从MID数据库拿到仓库数据
dbs_sql = """select * from wms_warehouse"""
dbs_df = pd.read_sql(dbs_sql, engine_dest)

# 判断取到的仓库执行时间是否是一个
# 对于只有一个执行时间的仓库
one_time = dbs_df[dbs_df.jiaoyun_time.str.len() <= 8]
two_time = dbs_df[dbs_df.jiaoyun_time.str.len() > 8]
print(two_time.jiaoyun_time.values.tolist())
print(t in two_time.jiaoyun_time.values.tolist())
print(str.split(two_time.jiaoyun_time.values.astype(str)))
# exc_df = one_time[(one_time.jiaoyun_time == t) | (t in str.split(two_time.jiaoyun_time.values.tostring()))]
# print(exc_df)
# for db in dbs_df.values:
#     exc_df = db['jiaoyun_time']
#     print(exc_df)
#     # if len(exc_df) == 8:
#     #     print(db.warehouse_desc)
#
#     # else:
#     #     time_list = db.jiaoyun_time
#     #     for exc_df in time_list:
#     #         print(exc_df)

#
# for i in range(0, len(sourceDBnamelist.split("::"))):
#     owms_sourceDB = sourceDBnamelist.split("::")[i]
#     owms_warehouseid = int(warehouseList.split("::")[i])
#     hms_sourceDB = hmsSourceDBnamelist.split("::")[i]
#
#     engine_source = ("mysql+pymysql://%s:%s@%s:%d/%s?charset=utf8" % (
#     fbg_owms_username, fbg_owms_password, fbg_owms_hostname, fbg_owms_port, owms_sourceDB))
#     engine_hms_source = ("mysql+pymysql://%s:%s@%s:%d/%s?charset=utf8" % (
#     fbg_hms_username, fbg_hms_password, fbg_hms_hostname, fbg_hms_port, hms_sourceDB))
#
#     # 1、获取仓库与北京时差（分冬令时和夏令时,夏令时比冬令时早一个小时）,仓库本地的截单时间，仓库当前日期
#
#     zone_sql = ("select case when saving_time=1 then (7-timezone) else (8-timezone) end as time_zone, "
#                 "substr(local_time,1,8) local_cut, "
#                 "WEEKDAY('%s')+1 as dayofweek "
#                 "from warehouse w  "
#                 "join wh_cut_off_time wc on wc.warehouse_code =w.warehouse_code ;" % (warehouse_date))
#
#     zone_df = cf.sql_to_df(engine_source, zone_sql)
#
#     # 仓库与中国时差，比中国晚的小时
#     time_zone = zone_df['time_zone'][0]
#     # 仓库本地截单时分秒
#     local_cut = zone_df['local_cut'][0]
#     # 获取统计日期的星期数，周末不统计，周一要把周末的都统计进来
#     dayofweek = zone_df['dayofweek'][0]
#
#     # 周末不统计
#     if dayofweek < 6:
#         # 2、定义脚本跑数日期:先获取仓库当前日期的截单时间，转成北京时间
#         # 仓库当前日期截单时间
#         warehouse_cut = datetime.datetime.strptime(warehouse_date + ' ' + local_cut, '%Y-%m-%d %H:%M:%S')
#         # 仓库当前日期截单时间转北京日期
#         beijing_cut = warehouse_cut + datetime.timedelta(hours=time_zone)
#         # 北京昨天截单时间
#         if dayofweek == 1:
#             # 周一的时候要把周六周日的都统计
#             yesterday = beijing_cut + datetime.timedelta(days=-3)
#             yesterday_local = warehouse_cut + datetime.timedelta(days=-3)
#
#         else:
#             yesterday = beijing_cut + datetime.timedelta(days=-1)
#             yesterday_local = warehouse_cut + datetime.timedelta(days=-1)
#
#         dtdate = str(beijing_cut)  # 当天北京截单时间
#         dtyesterday = str(yesterday)  # 昨天北京截单时间
#         dtyesterday_local = str(yesterday_local)  # 昨天仓库截单时间
#
#         # 表一：同步owms订单表
#         owms_dstable = 'temp_owms_orders'
#         # 1、删除目标表中当前仓库的数据
#         delete_sql = ("delete from %s.%s where warehouse_id=%s" % (dbdest, owms_dstable, owms_warehouseid))
#         # 调用函数来操作数据库，删除数据
#         cf.sql_caozuo(delete_sql, conn_dest)
#
#         # 2、查询源数据库数据
#         query_sql = (
#                 """select warehouse_id,wp_code,o.order_code,is_fba,
#                           order_status,sm_code,o.sc_code,'' as sort_code ,
#                           case when (oc.sc_code=o.sc_code and oc.status=1) then 1 else 0 end as sc_isopen,
#                           date_sub(call_wms_time,interval %s hour) sync_owms_time,
#                           date_sub(ship_time,interval %s hour) owms_ship_time
#                      from orders o
#                      join order_operation_time ot on o.order_id =ot.order_id
#                      join order_physical_relation opr on opr.order_code=o.order_code
#                      LEFT JOIN order_channel_test oc ON o.sc_code=oc.sc_code
#                     where call_wms_time>'%s'  and  call_wms_time<='%s'
#                       and order_status >=4 and o.customer_code not in('000010','000016')
#                       and sm_code not in ('PICKED_UP_BARTER','PICKED_UP_DESTROY','SHANG_PIN_CHAI_FEN','SHANG_PIN_HE_BING',
#                       'TUIJIANZIXUAN','WEIYUBAOTUIJIAN','ZITI') """ % (time_zone, time_zone, dtyesterday, dtdate))
#
#         # 结果保存在字典query_df中
#         query_df = cf.sql_to_df(engine_source, query_sql)
#
#         # 3、将查询出来的数据存入目标表
#         cf.df_to_sql(engine_dest, query_df, owms_dstable)
#
#         # 表二：同步表hms容器订单表
#         hms_dstable = 'temp_hms_container_details'
#         # 1、删除目标表中当前仓库的数据
#         delete_sql2 = ("delete from %s.%s where warehouse_id=%s" % (dbdest, hms_dstable, owms_warehouseid))
#         # 调用函数来操作数据库，删除数据
#         cf.sql_caozuo(delete_sql2, conn_dest)
#
#         # 2、查询源数据库数据
#         query_sql2 = (
#                     """select %s as warehouse_id,
#                               cd.order_number as order_code,
#                               cd.loader_time,
#                               cd.shipper_time,
#                               created_at
#                          from container c
#                          join container_details cd
#                            on c.container_id=cd.container_id
#                         where created_at>'%s'
#                         and created_at<date_add('%s',interval 1 day) """ % (owms_warehouseid, dtyesterday_local,
#                                                                             warehouse_date))
#
#         # 结果保存在字典query_df中
#         query_df2 = cf.sql_to_df(engine_hms_source, query_sql2)
#
#         # 3、将查询出来的数据存入目标表
#         cf.df_to_sql(engine_dest, query_df2, hms_dstable)
#
#         # 表三：同步owms历史已推送交运但未交运表
#         unfinish_dstable = 'temp_owms_orders_nottrans'
#         # 1、删除目标表中当前仓库的数据
#         delete_sql3 = ("delete from %s.%s where warehouse_id=%s" % (dbdest, unfinish_dstable, owms_warehouseid))
#         # 调用函数来操作数据库，删除数据
#         cf.sql_caozuo(delete_sql3, conn_dest)
#
#         # 2、查询源数据库数据
#         query_sql3 = (
#                     """select o.warehouse_id, wp_code,is_fba,o.sm_code,o.sc_code,
#                               o.order_code,o.customer_code,o.order_status,
#                               date_sub(call_wms_time,interval %s hour) sync_owms_time,
#                               date_sub(ship_time,interval %s hour) owms_ship_time
#                          from
#                               (select o.warehouse_id,is_fba,o.sm_code,o.sc_code,
#                                       o.order_code,o.customer_code,o.order_status,call_wms_time
#                                  from orders o
#                                  left join wuhan_pust_orders po on po.order_code=o.order_code
#                                 where call_wms_time>date_sub('%s',interval 15 day) and call_wms_time<='%s'
#                                   and po.order_code is null
#                                   and o.customer_code not in('000010','000016')
#                                   and sm_code not in ('PICKED_UP_BARTER','PICKED_UP_DESTROY','SHANG_PIN_CHAI_FEN',
#                                   'SHANG_PIN_HE_BING','TUIJIANZIXUAN','WEIYUBAOTUIJIAN','ZITI')
#                              )o
#                         join order_operation_time ot on ot.order_code=o.order_code
#                         join order_physical_relation opr on opr.order_code=o.order_code
#                         join orders_channel_container_complete occ on occ.order_code =o.order_code  """
#                     % (time_zone, time_zone, dtyesterday, dtyesterday))
#
#         # 结果保存在字典query_df中
#         query_df3 = cf.sql_to_df(engine_source, query_sql3)
#
#         # 3、将查询出来的数据存入目标表
#         cf.df_to_sql(engine_dest, query_df3, unfinish_dstable)
#
#         # 表四：目标表
#         dstable = 't_ret_order_transport_sum'
#         # 1、删除目标表中当前仓库的数据
#         delete_sql4 = ("delete from %s.%s where warehouse_id=%s and dtdate='%s'" % (
#         dbdest, dstable, owms_warehouseid, warehouse_date))
#         # 调用函数来操作数据库，删除数据
#         cf.sql_caozuo(delete_sql4, conn_dest)
#
#         # 2、查询源数据库数据，并插入目标表
#         query_sql4 = (
#                     """insert into t_ret_order_transport_sum(dtdate,create_time,warehouse_id,wp_code,is_fba,sm_code,sc_code,owms_cnt,sync_owms_cnt,sync_hms_cnt,finish_cnt,history_unfinish)
#                        select '%s' as dtdate,date_sub(now(),interval %s hour) as create_time,
#                               warehouse_id,wp_code,is_fba,sm_code,sc_code,
#                               sum(owms_cnt) as owms_cnt,
#                               sum(sync_owms_cnt) as sync_owms_cnt,
#                               sum(sync_hms_cnt) as sync_hms_cnt,
#                               sum(finish_cnt) as finish_cnt,
#                               sum(history_unfinish) as history_unfinish
#                         from
#                               (select o.warehouse_id,wp_code,is_fba,sm_code,sc_code,count(distinct o.order_code) as owms_cnt,
#                                       count(distinct case when sc_isopen =1 then o.order_code else null end ) sync_owms_cnt,
#                                       count(distinct c.order_code ) sync_hms_cnt,
#                                       count(distinct case when c.shipper_time<date_add('%s',interval 1 day) then c.order_code else null end ) finish_cnt,
#                                       0 as history_unfinish
#                                  from temp_owms_orders o
#                             left join temp_hms_container_details c
#                                    on o.order_code=c.order_code  and o.warehouse_id=c.warehouse_id
#                                 where o.warehouse_id =%s
#                                 group by o.warehouse_id,wp_code,is_fba,sm_code,sc_code
#                               union all
#                               select warehouse_id,wp_code,is_fba,sm_code,sc_code,
#                                      0 as owms_cnt,
#                                      0 sync_owms_cnt,
#                                      0 sync_hms_cnt,
#                                      0 finish_cnt,
#                                      count(distinct order_code) as history_unfinish
#                                 from temp_owms_orders_nottrans
#                                where warehouse_id =%s
#                                group by warehouse_id,wp_code,is_fba,sm_code,sc_code
#                               )t
#                         group by warehouse_id,wp_code,is_fba,sm_code,sc_code"""
#                     % (warehouse_date, time_zone, warehouse_date, owms_warehouseid, owms_warehouseid))
#         print(query_sql4)
#         # # 调用函数来操作数据库，插入数据
#         cf.sql_caozuo(query_sql4, conn_dest)
#
#     else:
#         print("今天星期" + str(dayofweek))
#         sys.exit()
