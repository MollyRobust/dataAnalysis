import pandas as pd
from task import common_function as cf

filepath = 'parameter.env'

# 数据来源，数据库配置信息
fbg_source_hostname = cf.MySearch(filepath, 'mysql_fbg_wms_hostname')
fbg_source_port = int(cf.MySearch(filepath, 'mysql_fbg_wms_port'))
fbg_source_username = cf.MySearch(filepath, 'mysql_fbg_wms_username')
fbg_source_password = cf.MySearch(filepath, 'mysql_fbg_wms_password')
srcDb = 'wms_goodcang_com'
engine_source = ("mysql+pymysql://%s:%s@%s:%d/%s?charset=utf8" % (fbg_source_username, fbg_source_password,
                                                                  fbg_source_hostname, fbg_source_port, srcDb))

# 目标数据库配置信息
fbg_mid_hostname = cf.MySearch(filepath, 'mysql_fbg_mid_hostname')
fbg_mid_port = int(cf.MySearch(filepath, 'mysql_fbg_mid_port'))
fbg_mid_username = cf.MySearch(filepath, 'mysql_fbg_mid_username')
fbg_mid_password = cf.MySearch(filepath, 'mysql_fbg_mid_password')
dstDb = 'fbg_mid_dw'
chargedTb = 't_bsc_receive_charged'
noChargeTb = 't_bsc_receive_no_charged'

# 目标数据库连接字典
conn_dest = dict()
conn_dest["host"] = fbg_mid_hostname
conn_dest["port"] = fbg_mid_port
conn_dest["user"] = fbg_mid_username
conn_dest["passwd"] = fbg_mid_password
conn_dest["db"] = dstDb
conn_dest["charset"] = "utf8"
engine_dest = ("mysql+pymysql://%s:%s@%s:%d/%s?charset=utf8" % (fbg_mid_username, fbg_mid_password,
                                                                fbg_mid_hostname, fbg_mid_port, dstDb))

# 获取全部入库单信息
query_receiving = ("""
                   select gr.receiving_code, reservation_number, is_fight, cabinet_type, 
                          receiving_status, receiving_type, wp_code, warehouse_code, 
                          customer_code, receiving_shipping_type, container_number, 
                          tracking_number, first_sign_time, deliveried_time, shelved_time, actual_ship_mode
                     from gc_receiving gr left join 
                     (select receiving_code, reservation_number, is_fight, cabinet_type 
                        from reservation_order ro join reservation_order_detail rod on ro.ro_id = rod.ro_id ) a 
                          on gr.receiving_code = a.receiving_code 
                """)
print(query_receiving)
receive_df = pd.read_sql(query_receiving, engine_source)

# 获取全部入库费订单
query_charge = ("""
               select wsc_code, reference_code as receiving_code
                 from wh_storage_charges
                where wsc_type in (1,3)
               union all
               select wsc_code,receiving_code
                 from wh_storage_charges as wsc
                      left join
                      (SELECT receiving_code, reservation_number
                         from reservation_order ro join reservation_order_detail rod on ro.ro_id = rod.ro_id) b 
                      on wsc.reference_code = b.reservation_number
                where wsc_type = 2
               """)
print(query_charge)
charg_df = pd.read_sql(query_charge, engine_source)

# 第一种情况：已计费入库费
charged_df = receive_df.merge(charg_df, how='inner', on=["receiving_code"])
print(charged_df.columns)
print(charged_df)

# 第二种情况：未计费入库费--取差集
no_charged_df = receive_df.append(charged_df.drop(columns='wsc_code')).drop_duplicates(keep=False)
print(no_charged_df)

# 先删除表中旧数据
del_chargedTb = "delete from %s.%s" % (dstDb, chargedTb)
del_noChargeTb = "delete from %s.%s" % (dstDb, noChargeTb)
cf.sql_caozuo(del_chargedTb, conn_dest)
cf.sql_caozuo(del_noChargeTb, conn_dest)

# 写入结果表
cf.df_to_sql(engine_dest, charged_df, chargedTb)
cf.df_to_sql(engine_dest, no_charged_df, noChargeTb)