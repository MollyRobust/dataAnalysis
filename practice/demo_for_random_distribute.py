import pandas as pd
import datetime
from task import common_function as cf

filepath = '../task/parameter.env'

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
dstTb = 't_tmp_test_random_demo'

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

# wms查询订单号
wms_sql = """
        select order_code, customer_code
          from orders
         where add_time >= '2020-10-21'
         limit 10
         """
wms_df = pd.read_sql(wms_sql, engine_wms)
print(type(wms_df)) # <class 'pandas.core.frame.DataFrame'>
for index, dataline in wms_df.iterrows():
    # print(type(dataline)) #<class 'pandas.core.series.Series'>
    datalist = list(dataline)
    newdata = pd.DataFrame(dataline)
    print(newdata)
    print(type(newdata))
    # cf.df_to_sql(newdata, engine_dest, dstTb)

