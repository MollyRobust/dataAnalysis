
import pandas as pd
import datetime
from task import common_function as cf

filepath = 'parameter.env'


# 数据来源，数据库配置信息
fbg_bsc_hostname = cf.MySearch(filepath, 'mysql_fbg_bsc_hostname')
fbg_bsc_port = int(cf.MySearch(filepath, 'mysql_fbg_bsc_port'))
fbg_bsc_username = cf.MySearch(filepath, 'mysql_fbg_bsc_username')
fbg_bsc_password = cf.MySearch(filepath, 'mysql_fbg_bsc_password')
engine_bsc = ("mysql+pymysql://%s:%s@%s:%d/?charset=utf8" % (fbg_bsc_username, fbg_bsc_password,
                                                             fbg_bsc_hostname, fbg_bsc_port))

# 数据来源，数据库配置信息
fbg_wms_hostname = cf.MySearch(filepath, 'mysql_fbg_wms_hostname')
fbg_wms_port = int(cf.MySearch(filepath, 'mysql_fbg_wms_port'))
fbg_wms_username = cf.MySearch(filepath, 'mysql_fbg_wms_username')
fbg_wms_password = cf.MySearch(filepath, 'mysql_fbg_wms_password')
wmsDb = 'wms_goodcang_com'
engine_wms = ("mysql+pymysql://%s:%s@%s:%d/%s?charset=utf8" % (fbg_wms_username, fbg_wms_password,
                                                               fbg_wms_hostname, fbg_wms_port, wmsDb))

# 数据中间仓库，数据库配置信息
fbg_mid_hostname = cf.MySearch(filepath, 'mysql_fbg_mid_hostname')
fbg_mid_port = int(cf.MySearch(filepath, 'mysql_fbg_mid_port'))
fbg_mid_username = cf.MySearch(filepath, 'mysql_fbg_mid_username')
fbg_mid_password = cf.MySearch(filepath, 'mysql_fbg_mid_password')

# 目标数据库
dstDb = 'fbg_mid_dw'
dstTb = 't_tmp_bsc_wms_compare_order'

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

# # bsc中的订单数据
# bsc_sql1 = """
#         select wf.OrderNumber as order_code, min(wo.OrderAddTime) as order_time
#           from gc_bsc_amc_202009.warehousbillflow wf
#           left join gc_bsc_amc_202009.warehouseorder wo
#           on wf.OrderNumber = wo.OrderNumber
#           where wf.AddTime >='2020-09-20'
#             and wo.BusinessType = 'so'
#           group by wf.OrderNumber
# """
# bsc_sql2 = """
#         select wf.OrderNumber as order_code, min(wo.OrderAddTime) as order_time
#           from gc_bsc_amc_202010.warehousbillflow wf
#           left join gc_bsc_amc_202010.warehouseorder wo
#           on wf.OrderNumber = wo.OrderNumber
#           where wf.AddTime >='2020-09-20'
#             and wo.BusinessType = 'so'
#           group by wf.OrderNumber
# """
# bsc_df1 = pd.read_sql(bsc_sql1, engine_bsc)
# bsc_df2 = pd.read_sql(bsc_sql2, engine_bsc)
# bsc_df = pd.concat([bsc_df1, bsc_df2], axis=0)
# print(bsc_df)

# 查询mid中间库中的数据
mid_sql = """
        select order_code,
               order_time
          from t_tmp_bsc_wms_compare_order_tmp
"""
print(mid_sql)
bsc_df = pd.read_sql(mid_sql, engine_dest)
print(bsc_df)
start_time = bsc_df.order_time.min()

# wms查询订单号
wms_sql = """
        select order_code, 2 as ref
          from orders
         where add_time >= '%s'
         group by order_code
         """ % start_time
wms_df = pd.read_sql(wms_sql, engine_wms)
print(wms_df)

# 关联两个dataframe
merge_df = pd.merge(bsc_df, wms_df, on='order_code', how='left')
print(merge_df[merge_df.ref.isnull()])
res_df = merge_df[merge_df.ref.isnull()].drop(['order_time', 'ref'], axis=1)
print(res_df)
cf.df_to_sql(engine_dest, res_df, dstTb)
