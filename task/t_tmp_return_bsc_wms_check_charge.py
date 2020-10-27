import pandas as pd
import datetime
from impala.dbapi import connect
from impala.util import as_pandas
from task import common_function as cf

filepath = 'parameter.env'

# 数据来源，数据库配置信息
fbg_wms_hostname = cf.MySearch(filepath, 'mysql_fbg_wms_hostname')
fbg_wms_port = int(cf.MySearch(filepath, 'mysql_fbg_wms_port'))
fbg_wms_username = cf.MySearch(filepath, 'mysql_fbg_wms_username')
fbg_wms_password = cf.MySearch(filepath, 'mysql_fbg_wms_password')
wmsDb = 'wms_goodcang_com'
wmsTb = "after_sales_return_orders"
engine_wms = ("mysql+pymysql://%s:%s@%s:%d/%s?charset=utf8" % (fbg_wms_username, fbg_wms_password,
                                                               fbg_wms_hostname, fbg_wms_port, wmsDb))

# 数据中间仓库，数据库配置信息
fbg_mid_hostname = cf.MySearch(filepath, 'mysql_fbg_mid_hostname')
fbg_mid_port = int(cf.MySearch(filepath, 'mysql_fbg_mid_port'))
fbg_mid_username = cf.MySearch(filepath, 'mysql_fbg_mid_username')
fbg_mid_password = cf.MySearch(filepath, 'mysql_fbg_mid_password')

# 目标数据库
dstDb = 'fbg_mid_dw'
dstTb = 't_tmp_datas_for_check_charge_result'
# 构建目标数据库连接字典
conn_dest = dict()
conn_dest["host"] = fbg_mid_hostname
conn_dest["port"] = fbg_mid_port
conn_dest["user"] = fbg_mid_username
conn_dest["passwd"] = fbg_mid_password
conn_dest["db"] = dstDb
conn_dest["charset"] = "utf8"
engine_dest = ("mysql+pymysql://%s:%s@%s:%d/%s?charset=utf8" % (fbg_mid_username, fbg_mid_password,
                                                                fbg_mid_hostname, fbg_mid_port, dstDb))
mid_sql = """
        select return_number,
               order_number,
               track_number,
               warehouse_code,
               order_add_time,
               customer_amount,
               total_amount,
               charged_amount
          from t_tmp_datas_for_check_charge
"""
mid_df = pd.read_sql(mid_sql, engine_dest)
print(mid_df)

wms_sql = """
        select asro_code as return_number,
               case when asro_status = 0 then '客户新建预报'
                    when asro_status = 1 then '待收货'
                    when asro_status = 2 then '待确认'
                    when asro_status = 3 then '已确认'
                    when asro_status = 4 then '拣货中'
                    when asro_status = 5 then '拣货完成'
                    when asro_status = 6 then '质检中'
                    when asro_status = 7 then '质检完成'
                    when asro_status = 8 then '上架中'
                    when asro_status = 9 then '处理完成'
                    when asro_status = 10 then '废弃'
                    when asro_status = 11 then '差异'
                    when asro_status = 12 then '待暂存'
                    when asro_status = 13 then '收货完成'
                    when asro_status = 14 then '销毁' end as return_status
          from after_sales_return_orders
         where asro_add_time >= '2020-01-01'
           and asro_add_time<='2020-09-30'
           and sm_code = 'FEDEXG-RETURN'
"""
wms_df = pd.read_sql(wms_sql, engine_wms)
print(wms_df)
res_df = pd.merge(mid_df, wms_df, how='left', on=['return_number'])
print(res_df)
cf.df_to_sql(engine_dest, res_df, dstTb)