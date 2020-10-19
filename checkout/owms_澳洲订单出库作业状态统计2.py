#!/usr/bin/python3
# -*- coding:utf-8 -*-

import pandas as pd
import datetime
from task import common_function as cf

filepath = 'parameter.env'

# 数据中间仓库，数据库配置信息
fbg_mid_hostname = cf.MySearch(filepath, 'mysql_fbg_mid_hostname')
fbg_mid_port = int(cf.MySearch(filepath, 'mysql_fbg_mid_port'))
fbg_mid_username = cf.MySearch(filepath, 'mysql_fbg_mid_username')
fbg_mid_password = cf.MySearch(filepath, 'mysql_fbg_mid_password')

# 目标数据库
dbdest = 'fbg_mid_dw'
dstable = 't_ret_order_status_track_sum_tmp'
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

# 取得要执行时间的仓库
exc_df = dbs_df[dbs_df.out_run_time == t]
print(exc_df)

# 当天跑前一天的数据
end_date = datetime.date.today()
start_date = end_date + datetime.timedelta(days=-1)
warehouse_date = str(start_date)
print(warehouse_date)

# 数据来源，数据库配置信息
fbg_owms_hostname = cf.MySearch(filepath, 'mysql_fbg_owms_hostname')
fbg_owms_port = int(cf.MySearch(filepath, 'mysql_fbg_owms_port'))
fbg_owms_username = cf.MySearch(filepath, 'mysql_fbg_owms_username')
fbg_owms_password = cf.MySearch(filepath, 'mysql_fbg_owms_password')

for sourceDB, warehouseid in zip(exc_df.db_name, exc_df.warehouse_id):
    engine_source = ("mysql+pymysql://%s:%s@%s:%d/%s?charset=utf8" % (fbg_owms_username, fbg_owms_password,
                                                                      fbg_owms_hostname, fbg_owms_port, sourceDB))
    # 1、获取仓库与北京时差（分冬令时和夏令时,夏令时比冬令时早一个小时），仓库截单时间，统计日期(欧洲统计昨天，澳洲统计今天)
    zone_sql = ("""select case when saving_time = 1 then (7-timezone)
                          else (8-timezone) end as time_dif,
                          substr(local_time,1,8) local_cut
                     from %s.warehouse w
                     join %s.wh_cut_off_time wc
                       on wc.warehouse_code =w.warehouse_code ;""" % (sourceDB, sourceDB))
    print(zone_sql)
    zone_df = pd.read_sql(zone_sql, engine_source)
    print(zone_df)
    # 仓库与中国时差，比中国晚的小时
    time_dif = int(zone_df['time_dif'][0])

    # 当地的截单时间
    local_cut = zone_df['local_cut'][0]

    # 仓库统计日期截单时间
    warehouse_cut = datetime.datetime.strptime(warehouse_date + ' ' + local_cut, '%Y-%m-%d %H:%M:%S')

    # 仓库统计日期截单时间转北京日期
    beijing_cut = warehouse_cut + datetime.timedelta(hours=time_dif)

    # 北京统计日期前一天截单时间
    beijing_yesterday_cut = beijing_cut + datetime.timedelta(days=-1)

    dtdate = warehouse_date  # 统计日期
    dttoday = str(end_date)  # 统计日期后一天

    dtdate_cut = str(beijing_cut)  # 统计当天北京截单时间
    dtyesterday_cut = str(beijing_yesterday_cut)  # 统计前一天北京截单时间

    # 2、删除目标表中当前仓库当天的数据
    delete_sql = "delete from %s.%s where warehouse_id = %s and dtdate = '%s'" % (dbdest, dstable, warehouseid, dtdate)
    # 调用函数来操作数据库，删除数据
    cf.sql_caozuo(delete_sql, conn_dest)

    # 3、查询源数据库数据
    query_sql = (
            """select date_sub(now(),interval %s hour) as create_time,
                      warehouse_id ,
                      wp_code,
                      '%s' as dtdate,
                      is_fba,
                      sm_code,
                      sc_code,
                      sum(sync_cnt) as sync_cnt,
                      sum(finish_ontime_cnt) as finish_ontime_cnt,
                      sum(finish_delay_cnt) as finish_delay_cnt,
                      sum(delete_cnt) as delete_cnt,
                      sum(submit_cnt) as submit_cnt,
                      sum(print_cnt) as print_cnt,
                      sum(pack_cnt) as pack_cnt,
                      sum(not_off_cnt) as not_off_cnt ,
                      sum(off_error_cnt) as off_error_cnt ,
                      sum(untreated_abnormal_cnt) as untreated_abnormal_cnt ,
                      sum(finish_abnormal_cnt) as finish_abnormal_cnt ,
                      sum(not_pick_cnt) as not_pick_cnt ,
                      sum(not_distri_cnt) as not_distri_cnt ,
                      sum(history_unfinish_cnt) as history_unfinish_cnt ,
                      sum(history_finish_cnt) as history_finish_cnt
                 from
                      (select o.warehouse_id ,wp_code, is_fba, sm_code, sc_code,
                              count(o.order_id) sync_cnt,
                              count(case when order_status =8 and `ship_time` < date_add('%s',interval %s hour) then o.order_id else null end ) as finish_ontime_cnt,
                              count(case when order_status =8 and `ship_time` >= date_add('%s',interval %s hour) then o.order_id else null end ) as finish_delay_cnt,
                              count(case when order_status =0 and order_exception_type =0 then o.order_id else null end ) as delete_cnt,
                              count(case when order_status =4 then o.order_id else null end ) as submit_cnt,
                              count(case when order_status =5 and order_exception_type =0 then o.order_id else null end ) as print_cnt,
                              count(case when order_status =7 and order_exception_type =0 then o.order_id else null end ) as pack_cnt,
                              count(case when order_status =4 and order_advance_pickup = 0 then o.order_id else null end ) as not_off_cnt,
                              count(case when order_status =4 and order_advance_pickup = 2 then o.order_id else null end ) as off_error_cnt,
                              count(case when order_exception_type != 0 and order_status IN (0,5,6,7) and order_exception_status =1 then o.order_id else null end ) as untreated_abnormal_cnt,
                              count(case when order_exception_type != 0 and order_status =0 and order_exception_status =2 then o.order_id else null end ) as finish_abnormal_cnt,
                              count(case when order_status =5 and pd.pick_status=1 then o.order_id else null end ) as not_pick_cnt,
                              count(case when order_status =5 and pd.distri_status=0 then o.order_id else null end ) as not_distri_cnt,
                              0 as history_unfinish_cnt ,
                              0 as history_finish_cnt
                         from orders o
                         join order_operation_time ot on o.order_id =ot.order_id
                         join order_physical_relation opr on opr.order_code=o.order_code
                         join warehouse w on w.warehouse_id =o.warehouse_id
                         left join
                             (select order_id,pick_status,min(pd_status) distri_status
                                from picking_detail p
                               where pick_status=1 and p.pd_add_time>='%s'
                               group by order_id, pick_status
                             ) pd
                           on pd.order_id = o.order_id
                       where call_wms_time>'%s' and call_wms_time<='%s' and o.customer_code not in('000010','000016')
                       group by o.warehouse_id,wp_code,is_fba,sm_code,sc_code
                      union all
                      select o.warehouse_id,wp_code,is_fba,sm_code, sc_code,
                             0 as sync_cnt,
                             0 as finish_ontime_cnt,
                             0 as finish_delay_cnt,
                             0 as delete_cnt,
                             0 as submit_cnt,
                             0 as print_cnt,
                             0 as pack_cnt,
                             0 as not_off_cnt ,
                             0 as off_error_cnt ,
                             0 as untreated_abnormal_cnt ,
                             0 as finish_abnormal_cnt ,
                             0 as not_pick_cnt ,
                             0 as not_distri_cnt ,
                             count(1)  as history_unfinish_cnt ,
                             0 as history_finish_cnt
                        from orders o
                        join order_physical_relation opr
                          on opr.order_code=o.order_code
                       where call_wms_time<='%s' and order_status in (4,5,7) and o.customer_code not in('000010','000016')
                       group by o.warehouse_id,wp_code,is_fba,sm_code, sc_code
                      union all
                      select o.warehouse_id,wp_code,is_fba,sm_code, sc_code,
                             0 as sync_cnt,
                             0 as finish_ontime_cnt,
                             0 as finish_delay_cnt,
                             0 as delete_cnt,
                             0 as submit_cnt,
                             0 as print_cnt,
                             0 as pack_cnt,
                             0 as not_off_cnt ,
                             0 as off_error_cnt ,
                             0 as untreated_abnormal_cnt ,
                             0 as finish_abnormal_cnt ,
                             0 as not_pick_cnt ,
                             0 as not_distri_cnt ,
                             0 as history_unfinish_cnt ,
                             count(1) as history_finish_cnt
                        from order_operation_time ot
                        join orders o on o.order_id =ot.order_id
                        join order_physical_relation opr
                          on opr.order_code=o.order_code
                       where ship_time >=date_add('%s',interval %s hour) and call_wms_time<='%s' and order_status=8 and o.customer_code not in('000010','000016')
                       group by o.warehouse_id,wp_code,is_fba,sm_code, sc_code
                      )t
                group by warehouse_id,wp_code,is_fba,sm_code, sc_code"""
            % (time_dif, dtdate, dttoday, time_dif, dttoday, time_dif, dtyesterday_cut, dtyesterday_cut, dtdate_cut,
               dtyesterday_cut, dtdate, time_dif, dtyesterday_cut))
    print(query_sql)
    res_df = pd.read_sql(query_sql, engine_source)

    # 4、将查询出来的数据存入目标表
    cf.df_to_sql(engine_dest, res_df, dstable)
