#!/usr/bin/python3
# -*- coding:utf-8 -*-

import pandas as pd
import sys
import datetime

from task import common_function as cf

filepath = 'parameter.env'

# 数据来源，数据库配置信息
fbg_owms_hostname = cf.MySearch(filepath, 'mysql_fbg_owms_hostname')
fbg_owms_port = int(cf.MySearch(filepath, 'mysql_fbg_owms_port'))
fbg_owms_username = cf.MySearch(filepath, 'mysql_fbg_owms_username')
fbg_owms_password = cf.MySearch(filepath, 'mysql_fbg_owms_password')

# 数据中间仓库，数据库配置信息

fbg_mid_hostname = cf.MySearch(filepath, 'mysql_fbg_mid_hostname')
fbg_mid_port = int(cf.MySearch(filepath, 'mysql_fbg_mid_port'))
fbg_mid_username = cf.MySearch(filepath, 'mysql_fbg_mid_username')
fbg_mid_password = cf.MySearch(filepath, 'mysql_fbg_mid_password')

# 当天跑前一天的数据
dtenddate = datetime.date.today()
endDate = dtenddate + datetime.timedelta(days=-1)
warehouse_date = str(endDate)

# 统计日期字符串转日期
warehouse_date1 = datetime.datetime.strptime(warehouse_date, '%Y-%m-%d')
# 统计结束日期
end_date = warehouse_date1 + datetime.timedelta(days=1)

dtdate = warehouse_date
dtenddate = str(end_date)

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

# 从MID数据库拿到仓库数据
dbs_sql = """select * from wms_warehouse"""
dbs_df = pd.read_sql(dbs_sql, engine_dest)

for sourceDB, warehouseid in zip(dbs_df.db_name, dbs_df.warehouse_id):
    engine_source = ("mysql+pymysql://%s:%s@%s:%d/%s?charset=utf8" % (fbg_owms_username, fbg_owms_password,
                                                                      fbg_owms_hostname, fbg_owms_port, sourceDB))

    # 1、获取仓库与北京时差（分冬令时和夏令时,夏令时比冬令时早一个小时），仓库截单时间，统计日期(欧洲统计昨天，澳洲统计今天)
    zone_sql = ("select case when saving_time=1 then (7-timezone) else (8-timezone) end as beijing_zone"
                " from warehouse w ")
    zone_df = pd.read_sql(zone_sql, engine_source)
    # 仓库与中国时差，比中国晚的小时
    time_dif = zone_df['beijing_zone'][0]

    # 拣货主宽表
    dstable1 = 't_mid_picking'
    # 1、删除目标表中当前仓库当天和7天前的数据
    delete_sql = ("delete from %s where dtdate=date_sub('%s',interval 7 day) and warehouse_id=%s" % (dstable1, dtdate,
                                                                                                     warehouseid))
    delete_sql1 = ("delete from %s where dtdate='%s' and warehouse_id=%s" % (dstable1, dtdate, warehouseid))
    # 调用函数来操作数据库，删除数据
    # cf.sql_caozuo(delete_sql, conn_dest)
    # cf.sql_caozuo(delete_sql1, conn_dest)

    # 2、查询源数据库数据
    query_sql1 = """
            select distinct '%s' as dtdate,
                   p.warehouse_id,w.warehouse_code,wp.wp_code,p.picking_code,
                   u.user_code,u.user_name,'null' as user_workcode,
                   wr.wellen_code,wr.wellen_name ,wr.order_pick_type,
                   p.picking_type,p.picking_mode,p.sorting_mode,p.is_fba,lc_level_type,p.is_cross_warehouse,maximum_cargo_type,
                   picking_order_cnt,picking_item_cnt,
                   date_sub(picking_add_time ,interval %s hour) picking_add_time,
                   date_sub(bind_container_time ,interval %s hour) bind_container_time,
                   date_sub(sorting_time ,interval %s hour) sorting_time
              from picking p
                   join warehouse w on p.warehouse_id =w.warehouse_id
                   join picking_physical_relation wp on wp.picking_code=p.picking_code
                   left join new_wellen_task wt on wt.nwt_id=p.new_task_id
                   left join new_wellen_rule wr on wr.wellen_id=wt.wellen_id
                   left join user u on u.user_id=p.picker_id
                   where sorting_time>=date_add('%s',interval %s hour) and sorting_time <date_add('%s',interval %s hour)
                   and sorting_status =3 """ % (dtdate, time_dif, time_dif, time_dif, dtdate,
                                                time_dif, dtenddate, time_dif)
    # 结果保存在字典query_df中
    query_df1 = pd.read_sql(query_sql1, engine_source)
    print(query_df1)
    # cf.df_to_sql(engine_dest, query_df1, dstable1)

    # # 拣货明细宽表
    # dstable2 = 't_mid_picking_detail'
    # # 1、删除目标表中当前仓库当天和7天前的数据
    # # delete_sql2 = ("delete from %s where dtdate='%s' and warehouse_id=%s" % (dstable2, dtdate, warehouseid))
    # # delete_sql3 = ("delete from %s where dtdate=date_sub('%s',interval 7 day) and warehouse_id=%s" % (dstable2, dtdate,
    # #                                                                                                   warehouseid))
    # # 调用函数来操作数据库，删除数据
    # # cf.sql_caozuo(delete_sql2, conn_dest)
    # # cf.sql_caozuo(delete_sql3, conn_dest)
    #
    # # 2、查询源数据库数据
    # query_sql2 = ("""
    #             select '%s' as dtdate,
    #                    p.warehouse_id,l.wp_code,p.picking_code,pd.order_code,
    #                    date_sub(ot.ship_time,interval %s hour) ship_time,
    #                    pd.product_barcode,pdt.cargo_type,
    #                    pd.lc_code,wa.wa_type,l.lc_level,l.roadway,l.row,
    #                    pd.pick_quantity,
    #                    pd.pick_quantity*pdt.product_real_height*pdt.product_real_length*pdt.product_real_width as pick_volume,
    #                    pd.pick_quantity*pdt.product_real_weight as pick_weight,
    #                    date_sub(pt.picking_start_time,interval %s hour) picking_start_time ,
    #                    date_sub(pt.picking_end_time,interval %s hour) picking_end_time ,
    #                    date_sub(pt.sync_time,interval %s hour) sync_time ,
    #                    date_sub(pt.move_library_time,interval %s hour) move_library_time
    #               from picking p
    #               join picking_detail pd
    #                 on p.picking_id =pd.picking_id
    #               join order_operation_time ot
    #                 on ot.order_id =pd.order_id
    #               join location l
    #                 on l.lc_code=pd.lc_code
    #               join warehouse_area wa
    #                 on l.wa_code=wa.wa_code
    #               join product pdt
    #                 on pdt.product_id=pd.product_id
    #               left join picking_time pt
    #                 on pt.picking_code=p.picking_code and pt.lc_code=pd.lc_code and pt.product_barcode=pd.product_barcode
    #              where sorting_time>=date_add('%s',interval %s hour)
    #                and sorting_time <date_add('%s',interval %s hour)
    #                and sorting_status =3 """ % (dtdate, time_dif, time_dif, time_dif, time_dif, time_dif, dtdate,
    #                                             time_dif, dtenddate, time_dif))
    # print(query_sql2)
    # query_df2 = cf.sql_to_df(engine_source, query_sql2)
    # print(query_df2)

    # 3、将查询出来的数据存入目标表
    # cf.df_to_sql(engine_dest, query_df2, dstable2)

print("all success!")
