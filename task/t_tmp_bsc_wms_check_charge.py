import pandas as pd
import datetime
from impala.dbapi import connect
from impala.util import as_pandas
from task import common_function as cf

filepath = 'parameter.env'

# hive连接信息
hive_host = "192.168.79.111"  # 当前连接的机器名
hive_port = 10000
db = "tmp"
user = 'root'
password = 'root'
table = 't_tmp_orders_for_check'
# 创建hive连接
conn = connect(host=hive_host, port=hive_port, database=db, auth_mechanism='PLAIN',)

# 数据中间仓库，数据库配置信息
fbg_mid_hostname = cf.MySearch(filepath, 'mysql_fbg_mid_hostname')
fbg_mid_port = int(cf.MySearch(filepath, 'mysql_fbg_mid_port'))
fbg_mid_username = cf.MySearch(filepath, 'mysql_fbg_mid_username')
fbg_mid_password = cf.MySearch(filepath, 'mysql_fbg_mid_password')

# 目标数据库
dstDb = 'fbg_mid_dw'
dstTb = 't_tmp_orders_for_check_result'
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

# 数据来源，数据库配置信息
fbg_wms_hostname = cf.MySearch(filepath, 'mysql_fbg_wms_hostname')
fbg_wms_port = int(cf.MySearch(filepath, 'mysql_fbg_wms_port'))
fbg_wms_username = cf.MySearch(filepath, 'mysql_fbg_wms_username')
fbg_wms_password = cf.MySearch(filepath, 'mysql_fbg_wms_password')
wmsDb = 'wms_goodcang_com'
engine_wms = ("mysql+pymysql://%s:%s@%s:%d/%s?charset=utf8" % (fbg_wms_username, fbg_wms_password,
                                                               fbg_wms_hostname, fbg_wms_port, wmsDb))

# 取到需要做对比的基础数据
mid_sql1 = """
        select return_number,
               order_number,
               tracking_number
          from t_tmp_orders_for_check
"""
mid_df1 = pd.read_sql(mid_sql1, engine_dest)
print(mid_df1)

# 取wms中的退件单状态
wms_sql = """
        select asro_code as return_number,
               order_code as order_number,
               tracking_no as tracking_number,
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
         where asro_add_time >= '2018-08-31'
           and asro_add_time<='2020-09-20'
"""
wms_df = pd.read_sql(wms_sql, engine_wms)
res_df1 = pd.merge(mid_df1, wms_df, how='left', on=['return_number', 'order_number', 'tracking_number'])
print(res_df1)

# 取hive中导出的数据
mid_sql2 = """
select return_number,
       order_number,
       tracking_number,
       wms_status,
       bsc_status
  from t_tmp_datas_for_check_charge2
"""
mid_df2 = pd.read_sql(mid_sql2, engine_dest)
print(mid_df2)
res_df = pd.merge(res_df1, mid_df2, how='left', on=['return_number'])
print(res_df)

cf.df_to_sql(engine_dest, res_df, dstTb)
